# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

MsgFlux is a lightweight, in-process asynchronous messaging library for .NET 10.0. It implements a producer-consumer model with per-consumer delivery semantics (AtMostOnce / AtLeastOnce) and pluggable durable storage.

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

The library uses a channel-based pub/sub pattern with two delivery paths (in-memory and durable) unified behind `IMessageSource`:

- **EngineService** (`MsgFlux.Core/Consuming/EngineService.cs`): `BackgroundService` that consumes from all `IMessageSource`s via `await foreach`, acquires a global `SemaphoreSlim` slot, and dispatches to `IConsume<T>` consumers. Uses Polly for configurable retry (default: 3 retries, 200ms exponential backoff). Tracks in-flight tasks and awaits them on shutdown.
- **Publisher** (`MsgFlux.Core/Publishing/Publisher.cs`): Serializes messages (JSON + Brotli), generates Guid V7 message IDs, injects OpenTelemetry trace context, and routes to `DurableBuffer` (AtLeastOnce) or `InMemoryMessageSource` (AtMostOnce) based on consumer semantics.
- **DurableBuffer** (`MsgFlux.Core/Publishing/DurableBuffer.cs`): Batches durable messages and flushes to `IMessageStore` on threshold or interval. Restores batch on flush failure.
- **InMemoryMessageSource** (`MsgFlux.Core/InMemory/InMemoryMessageSource.cs`): Bounded `Channel<Message>` for AtMostOnce consumers with native backpressure.
- **PollingStoreSource** (`MsgFlux.Core/Durability/PollingStoreSource.cs`): Polls `IMessageStore` for unprocessed messages, defers claim to `OnProcessing` callback, deduplicates in-flight items, batches ack calls.
- **JsonSerializer** (`MsgFlux.Core/Serialization/JsonSerializer.cs`): `ISerializer` using System.Text.Json with Brotli compression and `RecyclableMemoryStreamManager` for pooled allocations.
- **Registry** (`MsgFlux.Core/Consuming/Registry.cs`): Maps message types to consumers with FNV-1a stable hash-based ConsumerIds.
- **MsgFluxOptions** (`MsgFlux.Core/Configuration/MsgFluxOptions.cs`): Fluent configuration for all options.
- **Extensions** (`MsgFlux.Core/Configuration/Extensions.cs`): `AddMsgFlux()` extension method with synchronous config validation.

**Message flow (AtMostOnce)**: `PublishAsync<T>()` → serialize + Brotli → `InMemoryMessageSource` channel → Engine `await foreach` → semaphore → deserialize → scoped `IConsume<T>` → `HandleAsync()` with Polly retry.

**Message flow (AtLeastOnce)**: `PublishAsync<T>()` → serialize + Brotli → `DurableBuffer` → batch flush to `IMessageStore` → `PollingStoreSource` polls → Engine `await foreach` → semaphore → `OnProcessing` (claim) → deserialize → scoped `IConsume<T>` → `HandleAsync()` with Polly retry → `OnAck` (batched).

**Observability**: OpenTelemetry distributed tracing via `ActivitySource` named "MsgFlux". Trace context propagated through message headers (traceparent, tracestate).

## Solution Structure

- **MsgFlux.Abstractions** — contracts (`IConsume<T>`, `IPublish`, `IMessageStore`, `Message`, `Semantics`)
- **MsgFlux.Core** — library (NuGet package)
- **MsgFlux.Core.Tests** — NUnit 4.4.0 tests
- **MsgFlux.Core.Benchmarks** — end-to-end benchmarks with Testcontainers PostgreSQL
- **MsgFlux.Postgres** — PostgreSQL `IMessageStore` provider (COPY bulk + multi-VALUES INSERT)
- **MsgFlux.Postgres.Tests** — PostgreSQL integration tests with Testcontainers
- **MsgFlux.Demo** — ASP.NET Core demo (order processing pipeline with event chaining)

## CI/CD

GitHub Actions (`.github/workflows/ci-cd.yml`): builds and tests on push/PR, publishes to NuGet on `v*` tags using `NUGET_API_KEY` secret.
