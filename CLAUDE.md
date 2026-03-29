# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

MsgFlux is a lightweight, in-process asynchronous messaging library for .NET 10.0. It implements a producer-consumer model using `System.Threading.Channels` for decoupled communication within a single application.

## Build & Test Commands

```bash
# Build
dotnet build --configuration Release

# Run all tests
dotnet test --no-build --configuration Release --verbosity normal

# Run a single test by name
dotnet test --no-build --configuration Release --filter "FullyQualifiedName~TestMethodName"

# Run benchmarks
dotnet run --project MsgFlux.Core.Benchmarks --configuration Release

# Run demo app
dotnet run --project MsgFlux.Demo
```

## Architecture

The library uses a channel-based pub/sub pattern with these core components:

- **Engine** (`MsgFlux.Core/Engine.cs`): `BackgroundService` that reads from channels, deserializes envelopes, resolves `IConsume<T>` implementations from DI, and dispatches messages concurrently via `Parallel.ForEachAsync`. Uses Polly for retry (3 retries, exponential backoff at 200ms).
- **Publisher** (`MsgFlux.Core/Publisher.cs`): Serializes messages, wraps them in `Envelope` with OpenTelemetry trace context headers, and writes to the appropriate channel via `IChannelRxTx`.
- **InMemoryRxTx** (`MsgFlux.Core/RxTx/InMemoryRxTx.cs`): `IChannelRxTx` implementation using `ConcurrentDictionary<string, Channel>` with bounded capacity and backpressure.
- **JsonSerializer** (`MsgFlux.Core/Serialization/JsonSerializer.cs`): `ISerializer` implementation using System.Text.Json with Brotli compression.
- **Registry** (`MsgFlux.Core/Registry.cs`): Tracks which message types have registered consumers, used by Engine for task discovery.
- **MsgFluxOptions** (`MsgFlux.Core/MsgFluxOptions.cs`): Fluent configuration (channel capacity, max parallelism, payload size limit, consumer registration via `AddConsumer<T>()`).
- **Extensions** (`MsgFlux.Core/Extensions.cs`): `AddMsgFlux()` extension method wiring everything into `IServiceCollection`.

**Message flow**: `PublishAsync<T>()` → serialize + Brotli compress → create `Envelope` with headers → write to bounded channel → Engine reads → `Parallel.ForEachAsync` → deserialize → resolve scoped `IConsume<T>` consumers → `HandleAsync()` with Polly retry.

**Observability**: OpenTelemetry distributed tracing via `ActivitySource` named "MsgFlux". Trace context propagated through envelope headers (traceparent, tracestate).

## Solution Structure

- **MsgFlux.Core** — library (NuGet package)
- **MsgFlux.Core.Tests** — NUnit 4.4.0 tests
- **MsgFlux.Core.Benchmarks** — BenchmarkDotNet benchmarks
- **MsgFlux.Demo** — ASP.NET Core demo (order processing pipeline with event chaining)

## CI/CD

GitHub Actions (`.github/workflows/ci-cd.yml`): builds and tests on push/PR, publishes to NuGet on `v*` tags using `NUGET_API_KEY` secret.
