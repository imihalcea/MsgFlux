# MsgFlux

MsgFlux is a lightweight in-process messaging library for .NET, designed to facilitate asynchronous communication between components via a producer-consumer model. It natively integrates resilience (via Polly) and observability (via OpenTelemetry).

## Features

*   **In-process message bus**: Decoupled communication between components.
*   **Pub/Sub Model**: Message publication and consumption via typed handlers.
*   **Built-in Resilience**: Uses [Polly](https://github.com/App-vNext/Polly) for retry management (automatic retries on failure).
*   **Observability**: OpenTelemetry support (ActivitySource "MsgFlux") for distributed tracing.
*   **Dependency Injection**: Seamless integration with `Microsoft.Extensions.DependencyInjection`.
*   **Asynchronous Processing**: Uses `System.Threading.Channels` for efficient, non-blocking processing.

## Installation

(To be completed with specific installation instructions, e.g., via NuGet if published)

## Usage

### 1. Configuration

Add MsgFlux to your service container in `Program.cs` or `Startup.cs`. Register your consumers explicitly via the options callback.

```csharp
using MsgFlux.Core;

// ...

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
        // Creation logic...
        var userId = Guid.NewGuid().ToString();

        // Publishing the event
        await _publisher.PublishAsync(new UserCreated(userId, request.Email));

        return Ok(new { UserId = userId });
    }
}
```

## Architecture

*   **Engine**: Hosted service (`BackgroundService`) that listens to channels and distributes messages to the appropriate consumers.
*   **Publisher**: Service responsible for serializing and sending messages into channels.
*   **Registry**: Maintains the list of message types and associated consumers.
*   **RxTx**: Abstraction over `System.Threading.Channels` for message transmission.

## Resilience

MsgFlux uses a default resilience pipeline configured with:
*   3 retry attempts.
*   Exponential backoff starting at 200ms.

## License

See the [LICENSE](LICENSE) file.
