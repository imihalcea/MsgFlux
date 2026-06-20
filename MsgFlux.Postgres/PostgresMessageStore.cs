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
        // Claim-on-fetch: the inner SELECT locks eligible rows with FOR UPDATE SKIP LOCKED and the
        // outer UPDATE flips them to Processing in the SAME transaction before RETURNing them.
        // Concurrent pollers — including separate instances scaled horizontally — therefore never
        // receive the same row: it is claimed atomically the moment it is read.
        //
        // Note: processed_at (the stale-processing clock) starts at claim time, i.e. at fetch, not
        // at dispatch. The caller bounds maxCount to its free capacity (MaxDOP - in-flight), so claimed
        // rows are dispatched almost immediately and processed_at stays close to actual dispatch time;
        // the stale clause then only fires for genuine crashes, not for queued-but-not-yet-dispatched
        // work. Still size StaleProcessingTimeout above the longest expected handle duration.
        //
        // Parameters are numbered $1 = Processing state, $2 = now (processed_at); $3.. are appended
        // in add-order below. The SET clause references $1/$2 even though it appears after the CTE.
        var sb = new StringBuilder("""
            WITH claimed AS (
                SELECT message_id, consumer_id
                FROM msgflux_messages
                WHERE (state IN (0, 3)
            """);

        var idx = 3;
        if (staleProcessingTimeout.HasValue)
            sb.Append(" OR (state = 1 AND processed_at < $").Append(idx++).Append(')');

        sb.Append(')');

        if (messageType != null)
            sb.Append(" AND message_type = $").Append(idx++);

        sb.Append(" ORDER BY created_at ASC LIMIT $").Append(idx)
          .Append("""
                 FOR UPDATE SKIP LOCKED
            )
            UPDATE msgflux_messages m
            SET state = $1, processed_at = $2
            FROM claimed c
            WHERE m.message_id = c.message_id AND m.consumer_id = c.consumer_id
            RETURNING m.message_id, m.consumer_id, m.payload, m.headers, m.message_type, m.state, m.retry_count, m.error_details, m.created_at, m.processed_at
            """);

        await using var cmd = dataSource.CreateCommand(sb.ToString());
        cmd.Parameters.AddWithValue((short)MessageState.Processing); // $1
        cmd.Parameters.AddWithValue(clock.UtcNow);                   // $2

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

    /// <summary>
    /// Advisory-lock key used to serialize purging across instances. Exposed so operators running
    /// other pg_advisory_lock calls in the same database can avoid colliding with this key.
    /// </summary>
    public const long PurgeAdvisoryLockKey = 0x4D5347464C5558; // ASCII "MSGFLUX"

    public async Task<int> PurgeCompletedAsync(TimeSpan olderThan, CancellationToken ct = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        // With many instances each running their own purge timer, a single transaction-scoped
        // advisory lock lets exactly one instance purge per cycle; the rest skip without blocking.
        // pg_try_advisory_xact_lock is non-blocking and auto-releases on commit/rollback.
        await using (var lockCmd = new NpgsqlCommand("SELECT pg_try_advisory_xact_lock($1)", conn, tx))
        {
            lockCmd.Parameters.AddWithValue(PurgeAdvisoryLockKey);
            var acquired = (bool)(await lockCmd.ExecuteScalarAsync(ct))!;
            if (!acquired)
            {
                await tx.RollbackAsync(ct);
                return 0;
            }
        }

        await using var cmd = new NpgsqlCommand("""
            DELETE FROM msgflux_messages
            WHERE state = $1 AND created_at < $2
            """, conn, tx);
        cmd.Parameters.AddWithValue((short)MessageState.Completed);
        cmd.Parameters.AddWithValue(clock.UtcNow - olderThan);
        var deleted = await cmd.ExecuteNonQueryAsync(ct);

        await tx.CommitAsync(ct);
        return deleted;
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
