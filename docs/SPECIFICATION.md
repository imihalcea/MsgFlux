# Spécification fonctionnelle — MsgFlux

## 1. Objet et portée

MsgFlux est une bibliothèque de messaging asynchrone **in-process** suivant un modèle **producer / consumer**. Elle permet à des composants d'une même application de communiquer par messages typés, de façon découplée, avec un choix de **delivery guarantee** par consumer.

Ce n'est **pas** un message broker réseau autonome : MsgFlux n'expose pas de protocole réseau propre et coordonne toujours via le process hôte ou un **store** partagé. En **at-most-once**, producers et consumers vivent dans le même process. En **at-least-once**, dès lors que plusieurs process partagent le même **store** (et les mêmes contrats .NET), un message publié par un process peut être consommé par un autre (competing consumers) : le périmètre franchit alors les frontières de process. La durabilité (survie aux crashes) repose sur ce **store** de persistance externe optionnel.

---

## 2. Concepts du domaine

### 2.1 Message
Unité d'échange. Fonctionnellement, un message porte :
- un **message ID** unique (stable à travers les replays) ;
- un **payload** typé fourni par le producer ;
- un **message type** logique (qui pilote le routing) ;
- des **headers** clé-valeur (dont le trace context) ;
- un **state** de cycle de vie, un **retry count**, et des timestamps de création / traitement ;
- l'identité du **consumer** destinataire (consumer ID).

### 2.2 Consumer
Composant qui déclare traiter un message type donné et reçoit les messages correspondants un par un, en asynchrone, avec support de la cancellation.

### 2.3 Delivery semantics
Choisie **par consumer** à l'enregistrement :

| Semantics | Garantie | Coût | Effet d'un crash |
|---|---|---|---|
| **At-most-once** (défaut) | Le message peut ne jamais être délivré | Aucune persistance | Message perdu |
| **At-least-once** | Délivré au moins une fois (doublons possibles après recovery) | Store requis | Message rejoué |

### 2.4 Message states
`Pending` → `Processing` → `Completed` (succès, terminal). Une erreur mène à `Failed` (rejouable), et l'épuisement des retries mène à `DeadLettered` (terminal, abandonné).

### 2.5 Scheduled delivery
Au lieu d'être livré immédiatement, un message peut être **planifié** pour une date/heure précise (`deliver-at`). Il reste en attente jusqu'à son échéance, puis rejoint le cycle de vie normal comme s'il venait d'être publié. Un message planifié est **annulable** tant qu'il n'a pas atteint son échéance.

---

## 3. Exigences fonctionnelles

### 3.1 Publish
Quand un producer publie un payload :

- **EF-P1** — Le système valide la taille du payload sérialisé contre une limite configurable ; en cas de dépassement, le publish **échoue immédiatement avec une erreur** (rien n'est délivré).
- **EF-P2** — Le système identifie tous les consumers enregistrés pour ce message type. **Si aucun** consumer n'est enregistré, le message est **droppé silencieusement** (warning loggé, pas d'erreur).
- **EF-P3** — Une **copie logique distincte** du message est destinée à **chaque** consumer enregistré (modèle fan-out : N consumers → N deliveries indépendantes).
- **EF-P4** — Chaque copie reçoit un message ID unique et stable, le consumer ID cible, et le trace context courant.
- **EF-P5** — **Atomicité du routing** : les copies destinées aux consumers at-least-once sont prises en charge **avant** celles at-most-once. Si la prise en charge durable échoue, **aucune** copie in-memory n'est enqueuée (pas de partial delivery) et l'erreur remonte au producer.
- **EF-P6** — **Backpressure in-memory** : si la file in-memory est saturée, le publish **attend** qu'un slot se libère plutôt que de perdre ou rejeter le message.
- **EF-P7** — **Durabilité au publish (group-commit)** : pour les copies at-least-once, le publish ne se termine qu'**après** la persistance effective dans le store. Le producer n'est jamais acquitté avant l'écriture durable ; il n'existe pas de fenêtre où un message « accepté » ne serait qu'en mémoire. Les publishes concurrents sont coalescés en un seul batch (le débit du batching est conservé) sans introduire de fenêtre de perte. Si la persistance échoue, l'erreur **remonte au producer** (qui republie) ; aucun retry interne silencieux.
- **EF-P8** — **Backpressure durable bornée** : le nombre de messages durables en attente de persistance est plafonné (`Max buffered messages`). Au-delà, le publish **attend** qu'une place se libère — la mémoire est bornée et le producer s'aligne sur le débit réel du store, plutôt que d'accumuler sans limite.

### 3.2 Consume et cycle de vie
- **EF-C1** — Le système délivre chaque message à son consumer cible en invoquant son handler avec le payload désérialisé.
- **EF-C2** — **Routing par consumer** : un message n'est délivré qu'au consumer dont il porte le consumer ID ; les multiples consumers d'un même type sont **indépendants** (l'un peut réussir pendant que l'autre échoue).
- **EF-C3** — **Processing timeout** : chaque handler est soumis à un délai max configurable ; son dépassement est traité comme un échec.

### 3.3 Résilience : retries, failures, dead-letter
- **EF-R1** — En cas d'erreur (ou de timeout), le système **retry** automatiquement la delivery, jusqu'à un nombre de tentatives configurable, avec un **backoff exponentiel** entre tentatives (à partir d'un délai de base configurable). Les retries sont transparents pour le producer.
- **EF-R2** — **At-most-once, après épuisement** : le message est droppé (perdu), l'erreur est loggée.
- **EF-R3** — **At-least-once, après épuisement** : le message est marqué **dead-lettered** et n'est plus délivré. Un message ayant déjà dépassé le seuil de replays n'est plus dispatché du tout.
- **EF-R4** — Un failure en mode durable laisse le message dans un state **rejouable** ; le retry count est conservé.

### 3.4 Durabilité (at-least-once)
- **EF-D1** — **Recovery après crash** : au redémarrage, les messages non terminés sont automatiquement retrouvés et rejoués. Aucun message accepté en at-least-once n'est perdu suite à un arrêt brutal.
- **EF-D2** — **Stale processing detection** : un message resté en `Processing` au-delà d'un timeout configurable est considéré comme abandonné et redevient éligible au replay.
- **EF-D3** — **Anti-doublon concurrent** : un même couple (message, consumer) ne peut être en cours de traitement qu'une seule fois simultanément, y compris en cas de re-fetch depuis le store.
- **EF-D4** — **Ack durable** : le succès d'un handler est persisté ; un ack n'est pas perdu si un crash survient juste après le succès du consumer.
- **EF-D5** — **Résilience du store** : une indisponibilité temporaire du store ne fait pas s'arrêter le système ; les opérations sont retentées selon un intervalle configurable.

### 3.5 Concurrence et fairness
- **EF-X1** — Le nombre de messages traités **simultanément** est plafonné par un degree of parallelism global configurable (défaut : nombre de cœurs CPU).
- **EF-X2** — **Fairness entre sources** : aucune message source (in-memory ou store) ne peut affamer les autres ; les slots de traitement sont partagés équitablement.

### 3.6 Graceful shutdown
- **EF-S1** — À l'arrêt de l'application, le système **attend la fin** de tous les handlers in-flight avant de s'arrêter (pas de message orphelin).
- **EF-S2** — Les states et acks en attente sont flushés et persistés avant l'arrêt complet. Le buffer de publication durable effectue son flush final pendant la phase d'arrêt ordonnée du host, **tant que le store est encore disponible** (pas de dépendance à l'ordre de disposal).
- **EF-S3** — Les message sources sont closes (plus aucun nouveau message accepté).

### 3.7 Maintenance / rétention
- **EF-M1** — Les messages `Completed` (durables) sont **purgés automatiquement** au-delà d'un âge configurable, à intervalle configurable, pour éviter l'accumulation dans le store.

### 3.8 Observabilité
- **EF-O1** — Le système émet un **span** au publish et un span au consume de chaque message.
- **EF-O2** — Le **trace context est propagé** du producer vers le consumer (à travers le store le cas échéant), reconstituant la chaîne end-to-end selon le standard W3C Trace Context.
- **EF-O3** — Les erreurs et timeouts sont marqués en error status sur le span de consume.

### 3.9 Scheduling (livraison différée)
Quand un producer planifie un message pour une date de livraison (`deliver-at`) :

- **EF-SC1** — Le producer peut **planifier** la livraison d'un message à une date/heure précise plutôt que de le livrer immédiatement.
- **EF-SC2** — La planification **retourne un identifiant** (le message ID) permettant l'annulation ultérieure.
- **EF-SC3** — **Durabilité requise** : la planification exige un chemin durable. Planifier **sans store**, ou pour un message type dont **aucun** consumer n'est at-least-once, **échoue avec une erreur explicite** (rien n'est planifié). Justification : un message différé doit survivre aux crashes pendant son attente.
- **EF-SC4** — **Survie en attente** : un message planifié est **persisté dès la planification** et survit aux redémarrages / crashes durant toute l'attente jusqu'à son échéance.
- **EF-SC5** — **Précision de livraison** : la livraison intervient **à l'échéance ou peu après** ; la latence haute est bornée par la cadence de vérification configurable. Le système **n'offre pas de précision temps réel**.
- **EF-SC6** — **Date passée** : une `deliver-at` dans le passé est **acceptée** et entraîne une livraison dès que possible (au prochain cycle de vérification).
- **EF-SC7** — **Fuseau horaire** : la date est interprétée et conservée en **UTC**.
- **EF-SC8** — **Fan-out planifié** : comme au publish, un message planifié pour un type à N consumers (at-least-once) produit **N deliveries indépendantes** à l'échéance.
- **EF-SC9** — **Bascule vers le cycle normal** : à l'échéance, le message rejoint le cycle de vie **at-least-once standard** (delivery, retries, dead-letter, recovery, observabilité) et devient indistinguable d'un message publié à cet instant. Les exigences EF-C*, EF-R*, EF-D* et EF-O* s'appliquent **dès l'échéance**.
- **EF-SC10** — **Annulation** : un message planifié peut être **annulé avant sa livraison** via son identifiant (toutes les copies fan-out encore en attente). L'annulation est **best-effort** : si le message a déjà atteint son échéance et est entré dans le pipeline de livraison, l'annulation est **sans effet** et le message sera livré. L'opération **indique** si elle a effectivement empêché la livraison.

---

## 4. Configuration (comportements paramétrables)

| Paramètre | Rôle fonctionnel | Défaut |
|---|---|---|
| Max payload size | Seuil de rejet au publish | 64 Ko |
| Channel capacity | Seuil de déclenchement de la backpressure in-memory | 1000 |
| Max degree of parallelism | Messages traités simultanément | Nb de cœurs CPU |
| Max retry attempts | Nombre de deliveries avant drop / dead-letter | 3 |
| Retry delay | Délai de base du backoff exponentiel entre tentatives | 200 ms |
| Max dead-letter retries | Seuil de mise en dead-letter (durable) | 3 |
| Buffer flush threshold / interval | Batching des writes durables au publish | 1 / immédiat |
| Max buffered messages | Plafond de messages durables en attente de persistance (backpressure au publish) | 1000 |
| Polling batch size | Volume relu par cycle de recovery | 500 |
| Stale processing timeout | Avant de considérer un message comme abandonné | 5 min |
| Purge older-than / interval | Rétention des messages `Completed` | 4 h / 1 h |
| Replay interval | Cadence de polling et de replay | 1 s |
| Scheduling check interval | Cadence de vérification des messages planifiés dus (borne la précision de livraison) | 1 s |
| Scheduled purge | Rétention des messages planifiés livrés / annulés ; réutilise *Purge older-than / interval* | 4 h / 1 h |

---

## 5. Règles de configuration (validation au démarrage)
- **EF-V1** — Tout type enregistré comme consumer **doit** implémenter le contrat de consume ; sinon l'enregistrement échoue.
- **EF-V2** — Si **au moins un** consumer est déclaré at-least-once, un **store doit être fourni**, faute de quoi la configuration est **rejetée avec une erreur explicite** (validation synchrone, au moment de l'enregistrement).

---

## 6. Extension points
- **EF-E1** — Le store durable est **pluggable** : tout provider respectant le contrat (persist, mark-as-processing, acknowledge, mark-as-failed, dead-letter, fetch-unprocessed, purge) peut être substitué.
- **EF-E2** — Un provider PostgreSQL est livré en standard ; l'utilisateur peut en fournir un autre (autre SGBD, ajout de chiffrement, politique de rétention propre, etc.).
- **EF-E3** — Le contrat du store impose l'**unicité** par couple (message ID, consumer ID) et une **claim atomique** des messages à traiter (pas de double-prise entre instances concurrentes).
- **EF-E4** — Le **stockage des messages planifiés est pluggable** au même titre que le store durable, et **distinct** de celui-ci : un message planifié n'entre dans le pipeline de livraison qu'à son échéance, sans coupler le chemin de delivery au scheduling.

---

## 7. Hypothèses et limites
- Le routing est un **fan-out par enregistrement** : pas de notion de topic dynamique ; la liste des consumers est figée à la configuration.
- En at-most-once, **aucune** garantie n'est offerte (perte possible au moindre incident), aucune persistance n'est mobilisée.
- En at-least-once, des **doublons** de delivery sont possibles après recovery (le consumer doit être idempotent).
- L'intégrité/confidentialité du payload n'est pas assurée par la bibliothèque (pas de signing ni d'encryption intégrés).
- Le scheduling offre une livraison **différée ponctuelle** (un message = une livraison) : pas de **récurrence** ni de planification de type cron, et pas de précision **temps réel** (la latence est bornée par la cadence de vérification).
