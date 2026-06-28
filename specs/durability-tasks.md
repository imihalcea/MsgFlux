# Tâches d'implémentation : Durabilité MsgFlux

## Phase 1 : MsgFlux.Abstractions

- [ ] **T01** Créer le projet `MsgFlux.Abstractions/MsgFlux.Abstractions.csproj` (net10.0, 0 dépendance externe)
- [ ] **T02** Créer `MsgFlux.Abstractions/MessageState.cs` (enum : Pending, Processing, Completed, Failed, DeadLettered)
- [ ] **T03** Créer `MsgFlux.Abstractions/PersistedMessage.cs` (record avec MessageId, Payload, Headers, MessageType, State, RetryCount, CreatedAt, ProcessedAt?, ErrorDetails?)
- [ ] **T04** Créer `MsgFlux.Abstractions/IMessageStore.cs` (interface : PersistAsync, MarkAsProcessingAsync, AcknowledgeAsync, MarkAsFailedAsync, DeadLetterAsync, FetchUnprocessedAsync, PurgeCompletedAsync)
- [ ] **T05** Ajouter le projet à `MsgFlux.sln`

## Phase 2 : Modifications MsgFlux.Core

- [ ] **T06** Ajouter la référence projet `MsgFlux.Abstractions` dans `MsgFlux.Core.csproj`
- [ ] **T07** Modifier `MsgFluxOptions.cs` : ajouter `DurabilityEnabled`, `StaleProcessingTimeout`, `MaxDeadLetterRetries`, méthodes fluent `WithDurability()` et `WithStaleProcessingTimeout()`
- [ ] **T08** Créer `MsgFlux.Core/DurablePublisher.cs` : decorator autour de Publisher, persist-then-enqueue via IMessageStore
- [ ] **T09** Modifier `Engine.cs` : ajouter paramètre `IMessageStore?` au constructeur, appeler `MarkAsProcessingAsync` / `AcknowledgeAsync` / `MarkAsFailedAsync` / `DeadLetterAsync` dans le dispatch
- [ ] **T10** Modifier `Engine.cs` : changer `SafeExecuteConsumerAsync` pour retourner `Task<bool>` (succès/échec)
- [ ] **T11** Créer `MsgFlux.Core/MessageReplayService.cs` : BackgroundService qui fetch les messages unprocessed au démarrage et les ré-injecte dans les channels
- [ ] **T12** Modifier `Extensions.cs` : registration conditionnelle (DurablePublisher + MessageReplayService si DurabilityEnabled, sinon Publisher seul)

## Phase 3 : MsgFlux.Postgres

- [ ] **T13** Créer le projet `MsgFlux.Postgres/MsgFlux.Postgres.csproj` (ref Abstractions + Npgsql 9.0.*)
- [ ] **T14** Définir le DDL (schéma `msgflux` + table `msgflux.messages` + index) dans `SchemaInitializer`
- [ ] **T15** Créer `MsgFlux.Postgres/PostgresOptions.cs` (AutoCreateSchema = true par défaut)
- [ ] **T16** Créer `MsgFlux.Postgres/PostgresMessageStore.cs` : implémentation IMessageStore avec NpgsqlDataSource, SELECT FOR UPDATE SKIP LOCKED
- [ ] **T17** Créer `MsgFlux.Postgres/SchemaInitializer.cs` : BackgroundService qui exécute le DDL au démarrage
- [ ] **T18** Créer `MsgFlux.Postgres/Extensions.cs` : méthode `AddMsgFluxPostgres(connectionString, configure?)` pour le DI
- [ ] **T19** Ajouter le projet à `MsgFlux.sln`

## Phase 4 : Tests

- [ ] **T20** Ajouter la ref `MsgFlux.Abstractions` dans `MsgFlux.Core.Tests.csproj`
- [ ] **T21** Tests unitaires `DurablePublisher` avec mock IMessageStore (persist appelé avant enqueue, exception store = pas d'enqueue)
- [ ] **T22** Tests unitaires `Engine` modifié (ack sur succès, fail sur échec consumer, dead-letter après max retries)
- [ ] **T23** Tests unitaires `MessageReplayService` (replay des messages unprocessed dans les channels)
- [ ] **T24** Vérifier que tous les tests existants passent sans régression (mode sans durabilité)

## Phase 5 : Vérification

- [ ] **T25** `dotnet build --configuration Release` compile les 3 projets sans erreur
- [ ] **T26** `dotnet test --configuration Release` passe (existants + nouveaux)
- [ ] **T27** Mettre à jour le README avec la documentation durabilité (optionnel)
