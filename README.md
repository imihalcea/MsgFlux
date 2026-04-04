# MsgFlux

MsgFlux is a lightweight in-process messaging library for .NET, designed to facilitate asynchronous communication between components via a producer-consumer model. It natively integrates resilience (via Polly) and observability (via OpenTelemetry).

## Features

*   **In-process message bus**: Decoupled communication between components.
*   **Pub/Sub Model**: Message publication and consumption via typed handlers.
*   **Built-in Resilience**: Uses [Polly](https://github.com/App-vNext/Polly) for retry management (automatic retries on failure).
*   **Observability**: OpenTelemetry support (ActivitySource "MsgFlux") for distributed tracing.
*   **Dependency Injection**: Seamless integration with `Microsoft.Extensions.DependencyInjection`.
*   **Asynchronous Processing**: Uses `System.Threading.Channels` for efficient, non-blocking processing.
*   **Opt-in Durability**: Persist messages to survive process crashes, with replay on startup and automatic purge.

## Installation

(To be completed with specific installation instructions, e.g., via NuGet if published)

## Usage

### 1. Configuration

Add MsgFlux to your service container in `Program.cs` or `Startup.cs`. Register your consumers explicitly via the options callback.

```csharp
using MsgFlux.Core;

builder.Services.AddMsgFlux(options =>
{
    options.AddConsumer<UserCreatedConsumer>();
});
```

### 2. Defining a Message

A message can be any class or record.

```csharp
public record UserCreated(string UserId, string Email);
```

### 3. Creating a Consumer

Implement the `IConsume<T>` interface to define how to process a message.

```csharp
using MsgFlux.Core;

public class UserCreatedConsumer : IConsume<UserCreated>
{
    private readonly ILogger<UserCreatedConsumer> _logger;

    public UserCreatedConsumer(ILogger<UserCreatedConsumer> logger)
    {
        _logger = logger;
    }

    public async Task HandleAsync(UserCreated message, CancellationToken ct)
    {
        _logger.LogInformation("New user created: {UserId}, Email: {Email}", message.UserId, message.Email);
        await Task.CompletedTask;
    }
}
```

### 4. Publishing a Message

Inject `IPublish` to send messages.

```csharp
using MsgFlux.Core;
using Microsoft.AspNetCore.Mvc;

[ApiController]
[Route("[controller]")]
public class UserController : ControllerBase
{
    private readonly IPublish _publisher;

    public UserController(IPublish publisher)
    {
        _publisher = publisher;
    }

    [HttpPost]
    public async Task<IActionResult> CreateUser([FromBody] CreateUserRequest request)
    {
        var userId = Guid.NewGuid().ToString();

        await _publisher.PublishAsync(new UserCreated(userId, request.Email));

        return Ok(new { UserId = userId });
    }
}
```

## Durability

By default, MsgFlux is purely in-memory. If the process crashes, messages in transit are lost. The durability layer adds opt-in persistence behind the `IMessageStore` abstraction.

### Enabling Durability with PostgreSQL

```csharp
using MsgFlux.Core;
using MsgFlux.Postgres;

builder.Services.AddMsgFlux(options =>
{
    options
        .WithDurability()
        .WithPurge(olderThan: TimeSpan.FromDays(3), interval: TimeSpan.FromMinutes(30))
        .AddConsumer<UserCreatedConsumer>();
});

builder.Services.AddMsgFluxPostgres("Host=localhost;Database=msgflux");
```

### How It Works

When durability is enabled:

1. **Persist-then-enqueue**: Messages are persisted to the store *before* being written to the in-memory channel. If the store is unavailable, the publish fails and the message is not enqueued.
2. **Acknowledge on success**: After all consumers process a message successfully, it is marked as `Completed`.
3. **Mark as failed**: If a consumer fails after all retries, the message is marked as `Failed` with an incremented retry count.
4. **Replay on startup**: A `MessageReplayService` fetches all `Pending`, `Failed`, and stale `Processing` messages and re-injects them into the channels.
5. **Dead-letter**: During replay, messages that exceeded `MaxDeadLetterRetries` are moved to `DeadLettered` state instead of being re-enqueued.
6. **Automatic purge**: A `MessagePurgeService` periodically deletes old `Completed` messages.

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithDurability()` | `false` | Enables the durability layer |
| `WithStaleProcessingTimeout(TimeSpan)` | 5 minutes | Messages stuck in `Processing` longer than this are considered stale and replayed |
| `MaxDeadLetterRetries` | 3 | Failed messages exceeding this count are dead-lettered on replay |
| `WithPurge(olderThan, interval)` | 7 days / 1 hour | Purge completed messages older than `olderThan`, checked every `interval` |

### Message Lifecycle

```
Pending ──▶ Processing ──▶ Completed ──▶ (purged)
                │
                ▼
             Failed ──▶ (replayed) ──▶ Processing ──▶ ...
                │
                ▼ (after MaxDeadLetterRetries)
           DeadLettered
```

### PostgreSQL Provider Options

```csharp
builder.Services.AddMsgFluxPostgres("Host=localhost;Database=msgflux", options =>
{
    options.AutoCreateSchema = true; // default: true, creates table and indexes on startup
});
```

The provider uses `SELECT ... FOR UPDATE SKIP LOCKED` for safe multi-instance replay and `ON CONFLICT DO NOTHING` for idempotent persistence.

### Custom Providers

Implement `IMessageStore` from `MsgFlux.Abstractions` to plug in any storage backend. Register it before calling `AddMsgFlux`:

```csharp
builder.Services.AddSingleton<IMessageStore, MyCustomMessageStore>();
builder.Services.AddMsgFlux(options => options.WithDurability().AddConsumer<MyConsumer>());
```

## Architecture

```
MsgFlux.Abstractions   (0 external dependencies)
       ▲
       │
MsgFlux.Core ──ref──▶ MsgFlux.Abstractions
       ▲
       │
MsgFlux.Postgres ──ref──▶ MsgFlux.Abstractions + Npgsql
```

*   **Engine**: Hosted service (`BackgroundService`) that listens to channels and distributes messages to the appropriate consumers.
*   **Publisher / DurablePublisher**: Services responsible for serializing and sending messages into channels. `DurablePublisher` persists before enqueue.
*   **Registry**: Maintains the list of message types and associated consumers.
*   **RxTx**: Abstraction over `System.Threading.Channels` for message transmission.
*   **MessageReplayService**: Replays unprocessed messages on startup (durability mode).
*   **MessagePurgeService**: Periodically purges old completed messages (durability mode).

## Resilience

MsgFlux uses a default resilience pipeline configured with:
*   3 retry attempts.
*   Exponential backoff starting at 200ms.

## License

See the [LICENSE](LICENSE) file.
