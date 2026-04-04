using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;

namespace MsgFlux.Postgres.Tests;

[SetUpFixture]
public class PostgresFixture
{
    private static PostgreSqlContainer _container = null!;
    public static NpgsqlDataSource DataSource { get; private set; } = null!;
    public static string ConnectionString => _container.GetConnectionString();

    [OneTimeSetUp]
    public async Task GlobalSetup()
    {
        _container = new PostgreSqlBuilder()
            .WithImage("postgres:17-alpine")
            .Build();

        await _container.StartAsync();
        DataSource = NpgsqlDataSource.Create(_container.GetConnectionString());

        // Use SchemaInitializer to create the schema
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
