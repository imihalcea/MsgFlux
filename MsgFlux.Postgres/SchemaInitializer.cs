using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace MsgFlux.Postgres;

public partial class SchemaInitializer(
    NpgsqlDataSource dataSource,
    PostgresOptions options,
    ILogger<SchemaInitializer> logger) : BackgroundService
{
    private const string SchemaSql = """
        CREATE TABLE IF NOT EXISTS msgflux_messages (
            id            BIGSERIAL    PRIMARY KEY,
            message_id    TEXT         NOT NULL,
            consumer_id   TEXT         NOT NULL,
            payload       BYTEA        NOT NULL,
            headers       JSONB        NOT NULL DEFAULT '{}',
            message_type  TEXT         NOT NULL,
            state         SMALLINT     NOT NULL DEFAULT 0,
            retry_count   INT          NOT NULL DEFAULT 0,
            error_details TEXT,
            created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
            processed_at  TIMESTAMPTZ,
            UNIQUE (message_id, consumer_id)
        );

        CREATE INDEX IF NOT EXISTS ix_msgflux_unprocessed
            ON msgflux_messages (consumer_id, state, message_type, created_at) WHERE state IN (0, 1, 3);
        CREATE INDEX IF NOT EXISTS ix_msgflux_purge
            ON msgflux_messages (created_at) WHERE state = 2;
        """;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!options.AutoCreateSchema) return;

        try
        {
            await using var cmd = dataSource.CreateCommand(SchemaSql);
            await cmd.ExecuteNonQueryAsync(stoppingToken);
            LogSchemaCreated(logger);
        }
        catch (Exception ex)
        {
            LogSchemaCreationFailed(logger, ex);
            throw;
        }
    }

    [LoggerMessage(LogLevel.Information, "MsgFlux PostgreSQL schema created/verified")]
    static partial void LogSchemaCreated(ILogger logger);

    [LoggerMessage(LogLevel.Critical, "Failed to create MsgFlux PostgreSQL schema")]
    static partial void LogSchemaCreationFailed(ILogger logger, Exception ex);
}
