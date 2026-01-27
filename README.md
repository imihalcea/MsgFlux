# MsgFlux

MsgFlux est une bibliothèque légère de messagerie in-process pour .NET, conçue pour faciliter la communication asynchrone entre composants via un modèle producteur-consommateur. Elle intègre nativement la résilience (via Polly) et l'observabilité (via OpenTelemetry).

## Fonctionnalités

*   **Bus de messages in-process** : Communication découplée entre composants.
*   **Modèle Pub/Sub** : Publication de messages et consommation via des gestionnaires typés.
*   **Résilience intégrée** : Utilisation de [Polly](https://github.com/App-vNext/Polly) pour la gestion des retries (tentatives automatiques en cas d'échec).
*   **Observabilité** : Support d'OpenTelemetry (ActivitySource "MsgFlux") pour le traçage distribué.
*   **Injection de dépendances** : Intégration transparente avec `Microsoft.Extensions.DependencyInjection`.
*   **Traitement asynchrone** : Utilisation de `System.Threading.Channels` pour un traitement efficace et non bloquant.

## Installation

(À compléter avec les instructions d'installation spécifiques, ex: via NuGet si publié)

## Utilisation

### 1. Configuration

Ajoutez MsgFlux à votre conteneur de services dans `Program.cs` ou `Startup.cs`. Vous devez spécifier les assemblages contenant vos consommateurs.

```csharp
using MsgFlux.Core;

// ...

builder.Services.AddMsgFlux(typeof(Program).Assembly);
```

### 2. Définition d'un message

Un message peut être n'importe quelle classe ou enregistrement (record).

```csharp
public record UserCreated(string UserId, string Email);
```

### 3. Création d'un consommateur

Implémentez l'interface `IConsume<T>` pour définir comment traiter un message.

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
        _logger.LogInformation("Nouvel utilisateur créé : {UserId}, Email : {Email}", message.UserId, message.Email);
        await Task.CompletedTask;
    }
}
```

### 4. Publication d'un message

Injectez `IPublish` pour envoyer des messages.

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
        // Logique de création...
        var userId = Guid.NewGuid().ToString();

        // Publication de l'événement
        await _publisher.PublishAsync(new UserCreated(userId, request.Email));

        return Ok(new { UserId = userId });
    }
}
```

## Architecture

*   **Engine** : Service hébergé (`BackgroundService`) qui écoute les canaux et distribue les messages aux consommateurs appropriés.
*   **Publisher** : Service responsable de la sérialisation et de l'envoi des messages dans les canaux.
*   **Registry** : Maintient la liste des types de messages et des consommateurs associés.
*   **RxTx** : Abstraction sur `System.Threading.Channels` pour la transmission des messages.

## Résilience

MsgFlux utilise une pipeline de résilience par défaut configurée avec :
*   3 tentatives de réessai (Retries).
*   Un délai exponentiel (Backoff) commençant à 200ms.

## Licence

Voir le fichier [LICENSE](LICENSE).
