using Microsoft.Extensions.Logging.Abstractions;
using MsgFlux.Postgres;
using Npgsql;
using Testcontainers.PostgreSql;

namespace MsgFlux.Integration.Tests;

/// <summary>
/// Shared PostgreSQL container for the integration suite. Starts one container for all tests,
/// creates the MsgFlux schema once, and exposes the connection string so each simulated
/// application instance can build its own <see cref="NpgsqlDataSource"/> against the same database.
/// </summary>
[SetUpFixture]
public class PostgresContainerFixture
{
    private static PostgreSqlContainer _container = null!;
    public static string ConnectionString { get; private set; } = null!;
    public static NpgsqlDataSource DataSource { get; private set; } = null!;

    [OneTimeSetUp]
    public async Task GlobalSetup()
    {
        _container = new PostgreSqlBuilder()
            .WithImage("postgres:17-alpine")
            .Build();

        await _container.StartAsync();
        ConnectionString = _container.GetConnectionString();
        DataSource = NpgsqlDataSource.Create(ConnectionString);

        var initializer = new SchemaInitializer(
            DataSource,
            new PostgresOptions { AutoCreateSchema = true },
            NullLogger<SchemaInitializer>.Instance);

        await initializer.StartAsync(CancellationToken.None);
        await initializer.ExecuteTask!;
    }

    [OneTimeTearDown]
    public async Task GlobalTeardown()
    {
        await DataSource.DisposeAsync();
        await _container.DisposeAsync();
    }
}
