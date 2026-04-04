# Spécification : Durabilité des messages MsgFlux

## Contexte

MsgFlux est aujourd'hui purement in-memory (`System.Threading.Channels`). Si le process crash, les messages en transit sont perdus. L'objectif est d'ajouter une couche de durabilité **opt-in** derrière une abstraction, avec PostgreSQL comme premier provider.

## Architecture cible

```
MsgFlux.Abstractions   (0 dépendances externes)
       ▲
       │
MsgFlux.Core ──ref──▶ MsgFlux.Abstractions
       ▲
       │ (aucune ref directe)
MsgFlux.Postgres ──ref──▶ MsgFlux.Abstractions + Npgsql
```

Le provider (Postgres) ne référence que `MsgFlux.Abstractions`, jamais `MsgFlux.Core`. La mécanique d'orchestration (persist-then-enqueue, acknowledge-after-consume, replay au démarrage) reste dans Core.

---

## Abstractions (MsgFlux.Abstractions)

### MessageState

```csharp
public enum MessageState
{
    Pending,        // Persisté mais pas encore dispatché
    Processing,     // Pris en charge par l'Engine
    Completed,      // Consumer(s) exécuté(s) avec succès
    Failed,         // Tous les retries épuisés
    DeadLettered    // Déplacé en dead-letter après seuil max
}
```

### PersistedMessage

```csharp
public record PersistedMessage
{
    public required string MessageId { get; init; }
    public required byte[] Payload { get; init; }
    public required Dictionary<string, string> Headers { get; init; }
    public required string MessageType { get; init; }
    public MessageState State { get; init; } = MessageState.Pending;
    public int RetryCount { get; init; }
    public DateTimeOffset CreatedAt { get; init; }
    public DateTimeOffset? ProcessedAt { get; init; }
    public string? ErrorDetails { get; init; }
}
```

### IMessageStore

```csharp
public interface IMessageStore
{
    Task<string> PersistAsync(PersistedMessage message, CancellationToken ct = default);
    Task MarkAsProcessingAsync(string messageId, CancellationToken ct = default);
    Task AcknowledgeAsync(string messageId, CancellationToken ct = default);
    Task MarkAsFailedAsync(string messageId, string errorDetails, CancellationToken ct = default);
    Task DeadLetterAsync(string messageId, string reason, CancellationToken ct = default);
    Task<IReadOnlyList<PersistedMessage>> FetchUnprocessedAsync(
        string? messageType = null, int maxCount = 100,
        TimeSpan? staleProcessingTimeout = null, CancellationToken ct = default);
    Task<int> PurgeCompletedAsync(TimeSpan olderThan, CancellationToken ct = default);
}
```

Rien ne bouge de `MsgFlux.Core` vers `Abstractions`. Les interfaces existantes (`IPublish`, `IConsume<T>`, `IChannelRxTx`, `ISerializer`, `Envelope`) restent dans Core.

---

## Modifications Core (MsgFlux.Core)

### MsgFluxOptions -- nouvelles propriétés

```csharp
public bool DurabilityEnabled { get; internal set; }
public TimeSpan StaleProcessingTimeout { get; set; } = TimeSpan.FromMinutes(5);
public int MaxDeadLetterRetries { get; set; } = 3;

public MsgFluxOptions WithDurability() { DurabilityEnabled = true; return this; }
public MsgFluxOptions WithStaleProcessingTimeout(TimeSpan timeout) { ... }
```

### DurablePublisher (decorator)

Pattern decorator autour de `Publisher` existant :
1. Sérialise le message
2. Persiste via `IMessageStore.PersistAsync()` -- si le store est indisponible, exception remontée, message NON envoyé
3. Délègue à `Publisher.PublishAsync()` pour l'enqueue dans le channel

Trade-off : double sérialisation en v1 (optimisable ensuite).

### Engine -- acknowledge/fail/dead-letter

- Nouveau paramètre constructeur : `IMessageStore?` (nullable, null si durabilité off)
- `DispatchAsync` : appelle `MarkAsProcessingAsync` avant dispatch, `AcknowledgeAsync` si tous les consumers réussissent, `MarkAsFailedAsync` sinon
- `SafeExecuteConsumerAsync` : retourne `Task<bool>` au lieu de `Task` pour remonter le statut
- Dead-letter : si `RetryCount > MaxDeadLetterRetries`, appelle `DeadLetterAsync`

### MessageReplayService (nouveau BackgroundService)

Au démarrage, fetch les messages `Pending` + `Failed` + `Processing` stale, les ré-injecte dans les channels. Enregistré avant Engine dans le DI.

### Extensions.cs -- registration conditionnelle

- Si `DurabilityEnabled` : `IPublish` → `DurablePublisher` (wraps Publisher) + enregistre `MessageReplayService`
- Sinon : `IPublish` → `Publisher` (inchangé)

---

## PostgreSQL Provider (MsgFlux.Postgres)

### Dépendances

- `MsgFlux.Abstractions` (project ref)
- `Npgsql` 9.0.* (pas de Dapper, lib légère)
- `Microsoft.Extensions.Hosting.Abstractions`
- `Microsoft.Extensions.DependencyInjection.Abstractions`

### Schéma SQL

```sql
CREATE TABLE IF NOT EXISTS msgflux_messages (
    message_id    TEXT         PRIMARY KEY,
    payload       BYTEA        NOT NULL,
    headers       JSONB        NOT NULL DEFAULT '{}',
    message_type  TEXT         NOT NULL,
    state         SMALLINT     NOT NULL DEFAULT 0,
    retry_count   INT          NOT NULL DEFAULT 0,
    error_details TEXT,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    processed_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_msgflux_unprocessed
    ON msgflux_messages (state, message_type) WHERE state IN (0, 2, 3);
CREATE INDEX IF NOT EXISTS ix_msgflux_purge
    ON msgflux_messages (created_at) WHERE state = 2;
```

### PostgresMessageStore

Implémente `IMessageStore` avec `NpgsqlDataSource` (connection pooling intégré). `FetchUnprocessedAsync` utilise `SELECT ... FOR UPDATE SKIP LOCKED` pour le multi-instance.

### Extension DI

```csharp
services.AddMsgFluxPostgres("Host=...;Database=msgflux", options =>
{
    options.AutoCreateSchema = true;
});
```

### SchemaInitializer

`BackgroundService` qui exécute le DDL au démarrage si `AutoCreateSchema = true`.

---

## Contrats et garanties

| Sujet | Comportement |
|-------|-------------|
| Store indisponible au publish | Exception remontée, message non envoyé |
| Store indisponible à l'ack | Message reste en `Processing`, rejouable via stale timeout |
| Idempotence store | `ON CONFLICT DO NOTHING` sur PersistAsync |
| Idempotence consumer | Responsabilité de l'utilisateur (MessageId disponible) |
| Ordering | Non garanti (Parallel.ForEachAsync). Replay trié par `created_at ASC` |
| Cleanup | `PurgeCompletedAsync(TimeSpan)` à appeler manuellement ou via job |
| Multi-instance | `SELECT ... FOR UPDATE SKIP LOCKED` dans Postgres |
| Rétro-compatibilité | Sans `WithDurability()`, comportement identique. Aucun breaking change |

---

## Usage final

```csharp
builder.Services.AddMsgFlux(options =>
{
    options
        .WithChannelCapacity(1000)
        .WithDurability()
        .AddConsumer<OrderCreatedConsumer>();
});

builder.Services.AddMsgFluxPostgres("Host=localhost;Database=msgflux");
```
