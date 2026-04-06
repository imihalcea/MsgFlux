# MsgFlux

Lightweight in-process messaging library for .NET 10. Decoupled async communication between components via typed producers and consumers, with opt-in durable delivery.

## Features

- **Pub/Sub** with typed messages and consumers (`IConsume<T>`)
- **Per-consumer delivery semantics**: `AtMostOnce` (in-memory, fire-and-forget) or `AtLeastOnce` (durable, persisted to a store)
- **Resilience**: configurable Polly retry with exponential backoff
- **Observability**: OpenTelemetry distributed tracing (`ActivitySource "MsgFlux"`)
- **Backpressure**: bounded channels with producer-side backpressure
- **Graceful shutdown**: in-flight dispatches are awaited before the host exits
- **Pluggable storage**: implement `IMessageStore` for any backend (PostgreSQL provider included)

## Quick start

### 1. Register MsgFlux

```csharp
builder.Services.AddMsgFlux(options =>
{
    options.AddConsumer<OrderCreatedHandler>();
    options.AddConsumer<PaymentHandler>();
});
```

### 2. Define a message

Any class or record works.

```csharp
public record OrderCreated(string OrderId, decimal Amount);
```

### 3. Implement a consumer

```csharp
public class OrderCreatedHandler(ILogger<OrderCreatedHandler> logger) : IConsume<OrderCreated>
{
    public Task HandleAsync(OrderCreated message, CancellationToken ct)
    {
        logger.LogInformation("Order {OrderId} received: {Amount}", message.OrderId, message.Amount);
        return Task.CompletedTask;
    }
}
```

Consumers are resolved from DI as **scoped** services. You can inject any dependency.

### 4. Publish a message

```csharp
public class OrderController(IPublish publisher) : ControllerBase
{
    [HttpPost]
    public async Task<IActionResult> Create([FromBody] CreateOrderRequest req)
    {
        await publisher.PublishAsync(new OrderCreated(Guid.NewGuid().ToString(), req.Amount));
        return Accepted();
    }
}
```

One publish fans out to all registered consumers for that message type. Each consumer gets its own copy.

## Delivery semantics

Each consumer is registered with a delivery guarantee:

```csharp
options.AddConsumer<NotificationHandler>();                          // AtMostOnce (default)
options.AddConsumer<PaymentHandler>(Semantics.AtLeastOnce);         // durable
```

| Semantic | Behavior | Requires store |
|----------|----------|----------------|
| `AtMostOnce` | In-memory channel, fire-and-forget. Lost on process crash. | No |
| `AtLeastOnce` | Persisted before dispatch. Replayed on failure. Dead-lettered after max retries. | Yes |

Different consumers of the **same message type** can have different semantics.

`AtLeastOnce` consumers **must be idempotent** — a message may be delivered more than once after a failure.

## Durable delivery with PostgreSQL

```csharp
// Register the store BEFORE AddMsgFlux
builder.Services.AddMsgFluxPostgres("Host=localhost;Database=myapp");

builder.Services.AddMsgFlux(options =>
{
    options
        .AddConsumer<AuditLogHandler>(Semantics.AtLeastOnce)
        .AddConsumer<NotificationHandler>(); // AtMostOnce, no store needed
});
```

The PostgreSQL provider auto-creates the required table and indexes on startup. Disable with:

```csharp
builder.Services.AddMsgFluxPostgres("...", opts => opts.AutoCreateSchema = false);
```

### How durable delivery works

1. **Publish**: messages are buffered and flushed to the store in batch (configurable threshold/interval)
2. **Poll**: a background loop fetches unprocessed messages every `ReplayInterval` (default 1s) and dispatches them to consumers
3. **Claim**: each message is marked `Processing` just before consumer invocation (deferred to minimize stale-timeout risk)
4. **Ack**: successful completions are batched and flushed to the store in a single round-trip at the next poll cycle
5. **Fail**: on failure the message is marked `Failed` immediately with retry count incremented
6. **Retry**: failed messages are picked up on the next poll cycle and re-dispatched (in-flight deduplication prevents duplicate dispatch)
7. **Dead-letter**: messages exceeding `MaxDeadLetterRetries` are moved to `DeadLettered` state
8. **Purge**: a background service periodically deletes old completed messages

### Message lifecycle

```
Pending --> Processing --> Completed --> (purged)
                |
                v
             Failed --> (re-polled) --> Processing --> ...
                |
                v  (MaxDeadLetterRetries exceeded)
           DeadLettered
```

### Custom store provider

Implement `IMessageStore` from `MsgFlux.Abstractions` and register it before `AddMsgFlux`:

```csharp
builder.Services.AddSingleton<IMessageStore, MyCustomStore>();
builder.Services.AddMsgFlux(options =>
{
    options.AddConsumer<MyHandler>(Semantics.AtLeastOnce);
});
```

## Configuration

All options have sensible defaults. Override via the fluent API:

```csharp
builder.Services.AddMsgFlux(options =>
{
    options
        .WithMaxDegreeOfParallelism(4)
        .WithRetry(maxAttempts: 5, delay: TimeSpan.FromMilliseconds(500))
        .WithStaleProcessingTimeout(TimeSpan.FromMinutes(2))
        .WithMaxDeadLetterRetries(5)
        .WithMaxPayloadSizeKb(128)
        .WithChannelCapacity(5000)
        .WithReplayInterval(TimeSpan.FromSeconds(10))
        .WithPollingBatchSize(100)
        .WithBufferedPublishing(
            flushInterval: TimeSpan.FromMilliseconds(100),
            flushThreshold: 50)
        .WithPurge(
            olderThan: TimeSpan.FromDays(3),
            interval: TimeSpan.FromMinutes(30))
        .AddConsumer<MyHandler>(Semantics.AtLeastOnce);
});
```

### Options reference

| Option | Default | Description |
|--------|---------|-------------|
| `MaxDegreeOfParallelism` | `ProcessorCount` | Global concurrency cap across all sources |
| `MaxRetryAttempts` | 3 | Polly retry attempts per dispatch |
| `RetryDelay` | 200ms | Base delay for exponential backoff |
| `StaleProcessingTimeout` | 5 min | Per-dispatch timeout; also used to detect stuck messages |
| `MaxDeadLetterRetries` | 3 | Failed messages beyond this count are dead-lettered |
| `MaxPayloadSizeKb` | 64 | Publish rejects payloads larger than this |
| `ChannelCapacity` | 1000 | Bounded channel size for AtMostOnce consumers |
| `ReplayInterval` | 1s | Polling interval for durable message replay and ack flush |
| `PollingBatchSize` | 500 | Max messages fetched per poll cycle |
| `BufferFlushThreshold` | 1 | Flush durable buffer when this many messages accumulate (1 = immediate) |
| `BufferFlushInterval` | 0 | Periodic flush interval (0 = only flush on threshold) |
| `PurgeOlderThan` | 4 hours | Purge completed messages older than this |
| `PurgeInterval` | 1 hour | How often the purge service runs |

## Resilience

Every dispatch is wrapped in a Polly retry pipeline with exponential backoff. Configure via `WithRetry`:

```csharp
options.WithRetry(maxAttempts: 3, delay: TimeSpan.FromMilliseconds(200));
```

If all retries are exhausted:
- **AtMostOnce**: the failure is logged and the message is dropped
- **AtLeastOnce**: the message is marked `Failed` and retried on the next poll cycle, up to `MaxDeadLetterRetries`

## Processing timeout

Every dispatch is bounded by `StaleProcessingTimeout` (default: 5 minutes). If a consumer exceeds this duration:

- The `CancellationToken` passed to `HandleAsync` is cancelled
- In durable mode, the message is marked as `Failed` and will be retried

Cancellation in .NET is cooperative. Consumers **must** observe the token (pass it to `await` calls, check `ct.IsCancellationRequested`) for the timeout to take effect.

## Observability

MsgFlux creates OpenTelemetry activities via `ActivitySource("MsgFlux")`:

- **Publish**: `PublishAsync` creates a `Producer` activity with trace context injected into message headers (`traceparent`, `tracestate`)
- **Dispatch**: `EngineService` creates a `Consumer` activity linked to the publish trace

To capture traces, add the MsgFlux source to your OpenTelemetry configuration:

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing.AddSource("MsgFlux"));
```

## Architecture

```
MsgFlux.Abstractions   (zero external dependencies)
       ^
       |
MsgFlux.Core           (Polly, RecyclableMemoryStream, Hosting/DI abstractions)
       ^
       |
MsgFlux.Postgres       (Npgsql)  -- optional
```

**Core components:**

- **EngineService** -- `BackgroundService` that consumes from all `IMessageSource`s, acquires a global semaphore slot, and dispatches to the matching `IConsume<T>` consumer
- **Publisher** -- serializes messages (JSON + Brotli), routes to `DurableBuffer` or `InMemoryMessageSource` based on consumer semantics
- **DurableBuffer** -- batches durable writes and flushes to `IMessageStore`; restores batch on failure
- **InMemoryMessageSource** -- bounded `Channel<Message>` for AtMostOnce consumers
- **PollingStoreSource** -- polls `IMessageStore` for unprocessed messages, deduplicates in-flight items, defers claim to dispatch time
- **MessagePurgeService** -- periodically purges old completed messages from the store
- **Registry** -- maps message types to consumers with stable FNV-1a hash-based consumer IDs

## Event chaining

Consumers can inject `IPublish` to publish new messages, creating processing pipelines:

```csharp
public class OrderCreatedHandler(IPublish publisher) : IConsume<OrderCreated>
{
    public async Task HandleAsync(OrderCreated message, CancellationToken ct)
    {
        // Process order...
        await publisher.PublishAsync(new InventoryReserved(message.OrderId), ct);
    }
}
```

## When to use MsgFlux

**Good fit:**

- Decoupling components within a single application (event-driven architecture without infrastructure)
- Background processing triggered by API calls (send email, generate PDF, sync to external system)
- Event chaining pipelines where one action triggers another
- Applications already using PostgreSQL that want durable messaging without adding a broker
- Moderate throughput requirements (up to ~15K durable msg/s, ~180K in-memory msg/s)

**Not a good fit:**

- Cross-process or cross-service communication (use RabbitMQ, Kafka, or a cloud broker)
- Very high throughput durable messaging (>50K msg/s — use a dedicated broker)
- Exactly-once delivery (MsgFlux provides at-least-once; consumers must be idempotent)
- Long-term message retention or audit log (completed messages are purged after 4 hours by default)

## Performance

Benchmarks measured end-to-end: publish + store persistence + polling + dispatch + consumer execution.

Environment: .NET 10, PostgreSQL 17 (Testcontainers), Ubuntu 25.10, Intel Core Ultra 9 275HX (24 cores), 64 GB RAM.

Default concurrency (`MaxDegreeOfParallelism` = ProcessorCount = 24):

| Mode | 100 msg | 1K msg | 5K msg |
|------|---------|--------|--------|
| AtMostOnce | ~112K msg/s | ~87K msg/s | ~181K msg/s |
| AtLeastOnce | ~1.7K msg/s | ~15K msg/s | ~17K msg/s |
| Mixed | ~1.5K msg/s | ~11K msg/s | ~14K msg/s |

Impact of `MaxDegreeOfParallelism` on AtLeastOnce throughput (5K messages):

| DOP | 100 msg | 1K msg | 5K msg |
|-----|---------|--------|--------|
| 1 | 1.7K | 14K | 16K |
| 2 | 1.7K | 15K | 16K |
| 4 | 1.6K | 12K | 15K |
| 24 | 1.7K | 15K | 17K |

### Interpretation

**AtMostOnce** throughput scales with batch size (5K is ~60% faster than 1K) because the fixed cost of channel setup and DI scoping is amortized over more messages. At ~180K msg/s, the bottleneck is JSON serialization + Brotli compression.

**AtLeastOnce at 100 messages** is consistently ~1.7K msg/s regardless of DOP. This is not a throughput limit — it is polling latency. Messages wait up to `ReplayInterval` (1s) to be picked up after the first empty poll. The actual processing is fast; the wait is structural.

**AtLeastOnce at 1K-5K messages** reaches 14K-17K msg/s. At this volume, the publish phase overlaps with the poll cycle, so messages are picked up while they are still being published. Batched claims and acks (one SQL round-trip per batch instead of per message) account for most of the throughput gain.

**DOP has surprisingly little impact on durable throughput.** Even DOP=1 achieves 16K msg/s on 5K messages. The bottleneck is PostgreSQL I/O (fetch + batch claim + batch ack = 3 round-trips per poll cycle), not consumer parallelism. The benchmark consumers are near-instant (`Task.CompletedTask`); real-world consumers with I/O-bound work would benefit more from higher DOP.

**Mixed mode** is bounded by the durable path. AtMostOnce consumers complete almost instantly and do not contend with durable dispatch.

### Design trade-offs

- **Polling, not push**: the durable path polls PostgreSQL at `ReplayInterval` (default 1s). Consumers control the pace — no prefetch buffer overflow. The trade-off is latency: a message may wait up to 1s before being picked up.
- **Batched claims and acks**: state transitions (Processing, Completed) are accumulated and flushed in batch before each poll cycle. This reduces DB round-trips from N to 1, at the cost of a short window (~1s) where a crash could cause re-delivery.
- **In-flight deduplication**: prevents duplicate dispatch when messages are re-fetched before being acknowledged, with no delay penalty on new messages.
- **No WAL**: the `DurableBuffer` holds messages in memory before flushing to the store. If the process crashes before a flush, those buffered messages are lost. The default `BufferFlushThreshold=1` (immediate flush) minimizes this window.
- **AtLeastOnce consumers must be idempotent**: a message may be delivered more than once after a crash or timeout. This is a standard messaging contract, not specific to MsgFlux.

## Known limitations

- **In-process only**: MsgFlux is not a distributed message broker. All producers and consumers run in the same process.
- **Payload size**: very large payloads should be stored externally with a reference in the message.
- **JSON serialization is intentional**: all messages are serialized with JSON + Brotli compression, even for the in-memory path. This is by design — it enforces that message types are serializable, making a future migration to an external broker (RabbitMQ, Kafka, etc.) seamless. No code change needed on the producer/consumer side.
- **Polling latency**: durable messages are not dispatched instantly — they wait for the next poll cycle (up to `ReplayInterval`).

## License

See the [LICENSE](LICENSE) file.
