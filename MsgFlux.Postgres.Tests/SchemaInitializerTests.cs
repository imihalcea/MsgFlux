using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;

namespace MsgFlux.Postgres.Tests;

public class SchemaInitializerTests
{
    [Test]
    public async Task SchemaInitializer_Should_Create_Table_When_AutoCreateSchema_Enabled()
    {
        // Use a fresh container to verify schema creation from scratch
        await using var container = new PostgreSqlBuilder()
            .WithImage("postgres:17-alpine")
            .Build();
        await container.StartAsync();

        await using var dataSource = NpgsqlDataSource.Create(container.GetConnectionString());

        var initializer = new SchemaInitializer(
            dataSource,
            new PostgresOptions { AutoCreateSchema = true },
            NullLogger<SchemaInitializer>.Instance);

        await initializer.StartAsync(CancellationToken.None);
        await initializer.ExecuteTask!;

        // Verify table exists by inserting a row
        await using var cmd = dataSource.CreateCommand("""
            INSERT INTO msgflux.messages (message_id, consumer_id, payload, message_type, created_at)
            VALUES ('00000000-0000-0000-0000-000000000001', 'consumer-a', E'\\x01', 'Test', now())
            """);
        Assert.DoesNotThrowAsync(async () => await cmd.ExecuteNonQueryAsync());
    }

    [Test]
    public async Task SchemaInitializer_Should_Skip_When_AutoCreateSchema_Disabled()
    {
        await using var container = new PostgreSqlBuilder()
            .WithImage("postgres:17-alpine")
            .Build();
        await container.StartAsync();

        await using var dataSource = NpgsqlDataSource.Create(container.GetConnectionString());

        var initializer = new SchemaInitializer(
            dataSource,
            new PostgresOptions { AutoCreateSchema = false },
            NullLogger<SchemaInitializer>.Instance);

        await initializer.StartAsync(CancellationToken.None);
        if (initializer.ExecuteTask != null)
            await initializer.ExecuteTask;

        // Table should NOT exist
        await using var cmd = dataSource.CreateCommand(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'msgflux' AND table_name = 'messages')");
        var exists = (bool)(await cmd.ExecuteScalarAsync())!;
        Assert.That(exists, Is.False);
    }
}
