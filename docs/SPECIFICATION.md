# Spécification fonctionnelle — MsgFlux

## 1. Objet et portée

MsgFlux est une bibliothèque de messaging asynchrone **in-process** suivant un modèle **producer / consumer**. Elle permet à des composants d'une même application de communiquer par messages typés, de façon découplée, avec un choix de **delivery guarantee** par consumer.

Ce n'est **pas** un message broker réseau : producers et consumers vivent dans le même process. La durabilité (survie aux crashes) repose sur un **store** de persistance externe optionnel.

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

---

## 5. Règles de configuration (validation au démarrage)
- **EF-V1** — Tout type enregistré comme consumer **doit** implémenter le contrat de consume ; sinon l'enregistrement échoue.
- **EF-V2** — Si **au moins un** consumer est déclaré at-least-once, un **store doit être fourni**, faute de quoi la configuration est **rejetée avec une erreur explicite** (validation synchrone, au moment de l'enregistrement).

---

## 6. Extension points
- **EF-E1** — Le store durable est **pluggable** : tout provider respectant le contrat (persist, mark-as-processing, acknowledge, mark-as-failed, dead-letter, fetch-unprocessed, purge) peut être substitué.
- **EF-E2** — Un provider PostgreSQL est livré en standard ; l'utilisateur peut en fournir un autre (autre SGBD, ajout de chiffrement, politique de rétention propre, etc.).
- **EF-E3** — Le contrat du store impose l'**unicité** par couple (message ID, consumer ID) et une **claim atomique** des messages à traiter (pas de double-prise entre instances concurrentes).

---

## 7. Hypothèses et limites
- Le routing est un **fan-out par enregistrement** : pas de notion de topic dynamique ; la liste des consumers est figée à la configuration.
- En at-most-once, **aucune** garantie n'est offerte (perte possible au moindre incident), aucune persistance n'est mobilisée.
- En at-least-once, des **doublons** de delivery sont possibles après recovery (le consumer doit être idempotent).
- L'intégrité/confidentialité du payload n'est pas assurée par la bibliothèque (pas de signing ni d'encryption intégrés).
