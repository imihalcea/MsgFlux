using System.Text.Json;
using MsgFlux.Abstractions;
using Npgsql;

namespace MsgFlux.Postgres;

public class PostgresMessageStore(NpgsqlDataSource dataSource, IClock clock) : IMessageStore
{
    public async Task<string> PersistAsync(PersistedMessage message, CancellationToken ct = default)
    {
        const string sql = """
            INSERT INTO msgflux_messages (message_id, payload, headers, message_type, state, retry_count, created_at)
            VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7)
            ON CONFLICT (message_id) DO NOTHING
            """;

        await using var cmd = dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue(message.MessageId);
        cmd.Parameters.AddWithValue(message.Payload);
        cmd.Parameters.AddWithValue(JsonSerializer.Serialize(message.Headers));
        cmd.Parameters.AddWithValue(message.MessageType);
        cmd.Parameters.AddWithValue((short)message.State);
        cmd.Parameters.AddWithValue(message.RetryCount);
        cmd.Parameters.AddWithValue(message.CreatedAt);

        await cmd.ExecuteNonQueryAsync(ct);
        return message.MessageId;
    }

    public async Task MarkAsProcessingAsync(string messageId, CancellationToken ct = default)
    {
        const string sql = """
            UPDATE msgflux_messages SET state = $1, processed_at = $2
            WHERE message_id = $3
            """;

        await using var cmd = dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue((short)MessageState.Processing);
        cmd.Parameters.AddWithValue(clock.UtcNow);
        cmd.Parameters.AddWithValue(messageId);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task AcknowledgeAsync(string messageId, CancellationToken ct = default)
    {
        const string sql = """
            UPDATE msgflux_messages SET state = $1, processed_at = $2
            WHERE message_id = $3
            """;

        await using var cmd = dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue((short)MessageState.Completed);
        cmd.Parameters.AddWithValue(clock.UtcNow);
        cmd.Parameters.AddWithValue(messageId);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task MarkAsFailedAsync(string messageId, string errorDetails, CancellationToken ct = default)
    {
        const string sql = """
            UPDATE msgflux_messages SET state = $1, error_details = $2, retry_count = retry_count + 1
            WHERE message_id = $3
            """;

        await using var cmd = dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue((short)MessageState.Failed);
        cmd.Parameters.AddWithValue(errorDetails);
        cmd.Parameters.AddWithValue(messageId);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task DeadLetterAsync(string messageId, string reason, CancellationToken ct = default)
    {
        const string sql = """
            UPDATE msgflux_messages SET state = $1, error_details = $2
            WHERE message_id = $3
            """;

        await using var cmd = dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue((short)MessageState.DeadLettered);
        cmd.Parameters.AddWithValue(reason);
        cmd.Parameters.AddWithValue(messageId);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<IReadOnlyList<PersistedMessage>> FetchUnprocessedAsync(
        string? messageType = null, int maxCount = 100,
        TimeSpan? staleProcessingTimeout = null, CancellationToken ct = default)
    {
        var paramIndex = 1;
        var sql = """
            SELECT message_id, payload, headers, message_type, state, retry_count, error_details, created_at, processed_at
            FROM msgflux_messages
            WHERE (state IN (0, 3)
            """;

        if (staleProcessingTimeout.HasValue)
        {
            sql += $" OR (state = 1 AND processed_at < ${paramIndex})";
            paramIndex++;
        }

        sql += ")";

        if (messageType != null)
        {
            sql += $" AND message_type = ${paramIndex}";
            paramIndex++;
        }

        sql += $" ORDER BY created_at ASC LIMIT ${paramIndex} FOR UPDATE SKIP LOCKED";

        await using var cmd = dataSource.CreateCommand(sql);

        if (staleProcessingTimeout.HasValue)
            cmd.Parameters.AddWithValue(clock.UtcNow - staleProcessingTimeout.Value);

        if (messageType != null)
            cmd.Parameters.AddWithValue(messageType);

        cmd.Parameters.AddWithValue(maxCount);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        var results = new List<PersistedMessage>();

        while (await reader.ReadAsync(ct))
        {
            results.Add(new PersistedMessage
            {
                MessageId = reader.GetString(0),
                Payload = (byte[])reader[1],
                Headers = JsonSerializer.Deserialize<Dictionary<string, string>>(reader.GetString(2))
                          ?? new Dictionary<string, string>(),
                MessageType = reader.GetString(3),
                State = (MessageState)reader.GetInt16(4),
                RetryCount = reader.GetInt32(5),
                ErrorDetails = reader.IsDBNull(6) ? null : reader.GetString(6),
                CreatedAt = reader.GetFieldValue<DateTimeOffset>(7),
                ProcessedAt = reader.IsDBNull(8) ? null : reader.GetFieldValue<DateTimeOffset>(8)
            });
        }

        return results;
    }

    public async Task<int> PurgeCompletedAsync(TimeSpan olderThan, CancellationToken ct = default)
    {
        const string sql = """
            DELETE FROM msgflux_messages
            WHERE state = $1 AND created_at < $2
            """;

        await using var cmd = dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue((short)MessageState.Completed);
        cmd.Parameters.AddWithValue(clock.UtcNow - olderThan);
        return await cmd.ExecuteNonQueryAsync(ct);
    }
}
