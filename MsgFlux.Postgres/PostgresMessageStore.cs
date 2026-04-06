using System.Text;
using System.Text.Json;
using MsgFlux.Abstractions;
using Npgsql;
using NpgsqlTypes;

namespace MsgFlux.Postgres;

public class PostgresMessageStore(NpgsqlDataSource dataSource, IClock clock, PostgresOptions options) : IMessageStore
{
    public Task PersistAsync(IReadOnlyList<Message> messages, CancellationToken ct = default)
    {
        if (messages.Count == 0) return Task.CompletedTask;

        return messages.Count < options.BulkInsertThreshold
            ? PersistSmallBatchAsync(messages, ct)
            : PersistBulkAsync(messages, ct);
    }

    public Task MarkAsProcessingAsync(Guid messageId, string consumerId, CancellationToken ct = default)
        => UpdateStateAsync(MessageState.Processing, messageId, consumerId, ct);

    public Task MarkAsProcessingBatchAsync(IReadOnlyList<(Guid MessageId, string ConsumerId)> items, CancellationToken ct = default)
        => UpdateStateBatchAsync(MessageState.Processing, items, ct);

    public Task AcknowledgeAsync(Guid messageId, string consumerId, CancellationToken ct = default)
        => UpdateStateAsync(MessageState.Completed, messageId, consumerId, ct);

    public Task AcknowledgeBatchAsync(IReadOnlyList<(Guid MessageId, string ConsumerId)> items, CancellationToken ct = default)
        => UpdateStateBatchAsync(MessageState.Completed, items, ct);

    public async Task MarkAsFailedAsync(Guid messageId, string consumerId, string errorDetails, CancellationToken ct = default)
    {
        const string sql = """
            UPDATE msgflux_messages SET state = $1, error_details = $2, retry_count = retry_count + 1
            WHERE message_id = $3 AND consumer_id = $4
            """;

        await using var cmd = dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue((short)MessageState.Failed);
        cmd.Parameters.AddWithValue(errorDetails);
        cmd.Parameters.AddWithValue(messageId);
        cmd.Parameters.AddWithValue(consumerId);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task DeadLetterAsync(Guid messageId, string consumerId, string reason, CancellationToken ct = default)
    {
        const string sql = """
            UPDATE msgflux_messages SET state = $1, error_details = $2
            WHERE message_id = $3 AND consumer_id = $4
            """;

        await using var cmd = dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue((short)MessageState.DeadLettered);
        cmd.Parameters.AddWithValue(reason);
        cmd.Parameters.AddWithValue(messageId);
        cmd.Parameters.AddWithValue(consumerId);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<IReadOnlyList<Message>> FetchUnprocessedAsync(
        string? messageType = null, int maxCount = 100,
        TimeSpan? staleProcessingTimeout = null, CancellationToken ct = default)
    {
        var paramIndex = 1;
        var sql = """
            SELECT message_id, consumer_id, payload, headers, message_type, state, retry_count, error_details, created_at, processed_at
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
        var results = new List<Message>();

        while (await reader.ReadAsync(ct))
        {
            results.Add(new Message
            {
                MessageId = reader.GetFieldValue<Guid>(0),
                ConsumerId = reader.GetString(1),
                Payload = (byte[])reader[2],
                Headers = JsonSerializer.Deserialize<Dictionary<string, string>>(reader.GetString(3))
                          ?? new Dictionary<string, string>(),
                MessageType = reader.GetString(4),
                State = (MessageState)reader.GetInt16(5),
                RetryCount = reader.GetInt32(6),
                ErrorDetails = reader.IsDBNull(7) ? null : reader.GetString(7),
                CreatedAt = reader.GetFieldValue<DateTimeOffset>(8),
                ProcessedAt = reader.IsDBNull(9) ? null : reader.GetFieldValue<DateTimeOffset>(9)
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

    private async Task UpdateStateAsync(MessageState state, Guid messageId, string consumerId, CancellationToken ct)
    {
        const string sql = """
            UPDATE msgflux_messages SET state = $1, processed_at = $2
            WHERE message_id = $3 AND consumer_id = $4
            """;

        await using var cmd = dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue((short)state);
        cmd.Parameters.AddWithValue(clock.UtcNow);
        cmd.Parameters.AddWithValue(messageId);
        cmd.Parameters.AddWithValue(consumerId);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task UpdateStateBatchAsync(MessageState state, IReadOnlyList<(Guid MessageId, string ConsumerId)> items, CancellationToken ct)
    {
        if (items.Count == 0) return;

        const string sql = """
            UPDATE msgflux_messages SET state = $1, processed_at = $2
            WHERE (message_id, consumer_id) IN (SELECT unnest($3), unnest($4))
            """;

        await using var cmd = dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue((short)state);
        cmd.Parameters.AddWithValue(clock.UtcNow);
        cmd.Parameters.AddWithValue(items.Select(i => i.MessageId).ToArray());
        cmd.Parameters.AddWithValue(items.Select(i => i.ConsumerId).ToArray());
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task PersistSmallBatchAsync(IReadOnlyList<Message> messages, CancellationToken ct)
    {
        var sb = new StringBuilder(
            "INSERT INTO msgflux_messages (message_id, consumer_id, payload, headers, message_type, state, retry_count, created_at) VALUES ");

        await using var cmd = dataSource.CreateCommand();
        var p = 1;
        for (var i = 0; i < messages.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append('(')
              .Append('$').Append(p++).Append(", ")
              .Append('$').Append(p++).Append(", ")
              .Append('$').Append(p++).Append(", ")
              .Append('$').Append(p++).Append("::jsonb, ")
              .Append('$').Append(p++).Append(", ")
              .Append('$').Append(p++).Append(", ")
              .Append('$').Append(p++).Append(", ")
              .Append('$').Append(p++).Append(')');

            var m = messages[i];
            cmd.Parameters.AddWithValue(m.MessageId);
            cmd.Parameters.AddWithValue(m.ConsumerId);
            cmd.Parameters.AddWithValue(m.Payload);
            cmd.Parameters.AddWithValue(JsonSerializer.Serialize(m.Headers));
            cmd.Parameters.AddWithValue(m.MessageType);
            cmd.Parameters.AddWithValue((short)m.State);
            cmd.Parameters.AddWithValue(m.RetryCount);
            cmd.Parameters.AddWithValue(m.CreatedAt);
        }
        sb.Append(" ON CONFLICT (message_id, consumer_id) DO NOTHING");

        cmd.CommandText = sb.ToString();
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task PersistBulkAsync(IReadOnlyList<Message> messages, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);

        await using var createTemp = new NpgsqlCommand("""
            CREATE TEMP TABLE IF NOT EXISTS _msgflux_bulk (
                message_id UUID, consumer_id TEXT, payload BYTEA,
                headers JSONB, message_type TEXT, state SMALLINT,
                retry_count INT, created_at TIMESTAMPTZ
            )
            """, conn);
        await createTemp.ExecuteNonQueryAsync(ct);

        await using var truncate = new NpgsqlCommand("TRUNCATE _msgflux_bulk", conn);
        await truncate.ExecuteNonQueryAsync(ct);

        await using var writer = await conn.BeginBinaryImportAsync(
            "COPY _msgflux_bulk (message_id, consumer_id, payload, headers, message_type, state, retry_count, created_at) FROM STDIN (FORMAT BINARY)",
            ct);

        foreach (var m in messages)
        {
            await writer.StartRowAsync(ct);
            await writer.WriteAsync(m.MessageId, NpgsqlDbType.Uuid, ct);
            await writer.WriteAsync(m.ConsumerId, NpgsqlDbType.Text, ct);
            await writer.WriteAsync(m.Payload, NpgsqlDbType.Bytea, ct);
            await writer.WriteAsync(JsonSerializer.Serialize(m.Headers), NpgsqlDbType.Jsonb, ct);
            await writer.WriteAsync(m.MessageType, NpgsqlDbType.Text, ct);
            await writer.WriteAsync((short)m.State, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync(m.RetryCount, NpgsqlDbType.Integer, ct);
            await writer.WriteAsync(m.CreatedAt, NpgsqlDbType.TimestampTz, ct);
        }

        await writer.CompleteAsync(ct);
        await writer.CloseAsync(ct);

        await using var merge = new NpgsqlCommand("""
            INSERT INTO msgflux_messages (message_id, consumer_id, payload, headers, message_type, state, retry_count, created_at)
            SELECT message_id, consumer_id, payload, headers, message_type, state, retry_count, created_at
            FROM _msgflux_bulk
            ON CONFLICT (message_id, consumer_id) DO NOTHING
            """, conn);
        await merge.ExecuteNonQueryAsync(ct);
    }
}
