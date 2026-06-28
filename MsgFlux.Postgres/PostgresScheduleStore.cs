using System.Text;
using MsgFlux.Abstractions;
using Npgsql;
using NpgsqlTypes;

namespace MsgFlux.Postgres;

/// <summary>
/// PostgreSQL <see cref="IScheduleStore"/>: cold storage for scheduled messages in
/// <c>msgflux.scheduled_messages</c>, kept separate from the hot path. One row per scheduled publish,
/// keyed by message_id; the content travels as the opaque <c>msg</c> blob.
/// </summary>
public class PostgresScheduleStore(NpgsqlDataSource dataSource, IClock clock, PostgresOptions options) : IScheduleStore
{
    /// <summary>
    /// Advisory-lock key serializing scheduled-message purges across instances. Distinct from the
    /// hot-path purge key so the two purges never block one another.
    /// </summary>
    public const long PurgeAdvisoryLockKey = 0x4D5347465343; // ASCII "MSGFSC"

    public Task ScheduleAsync(IReadOnlyList<ScheduledMessage> messages, CancellationToken ct = default)
    {
        if (messages.Count == 0) return Task.CompletedTask;

        return messages.Count < options.BulkInsertThreshold
            ? PersistSmallBatchAsync(messages, ct)
            : PersistBulkAsync(messages, ct);
    }

    public async Task<IReadOnlyList<ScheduledMessage>> FetchDueAsync(int maxCount = 100, CancellationToken ct = default)
    {
        // Plain claim by lock (not by state flip): promotion must persist to the hot path BEFORE the
        // row is marked promoted, otherwise a crash after the flip would lose the message. FOR UPDATE
        // SKIP LOCKED keeps concurrent promoters off the same rows while they are read; double-fetch
        // across separate cycles is still harmless because promotion is idempotent.
        const string sql = """
            SELECT message_id, msg, message_type, state, deliver_at, created_at
            FROM msgflux.scheduled_messages
            WHERE state = 0 AND deliver_at <= $1
            ORDER BY deliver_at ASC, created_at ASC
            LIMIT $2
            FOR UPDATE SKIP LOCKED
            """;

        await using var cmd = dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue(clock.UtcNow);
        cmd.Parameters.AddWithValue(maxCount);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        var results = new List<ScheduledMessage>();

        while (await reader.ReadAsync(ct))
        {
            results.Add(new ScheduledMessage
            {
                Id = reader.GetFieldValue<Guid>(0),
                Msg = (byte[])reader[1],
                Type = reader.GetString(2),
                Status = (ScheduledState)reader.GetInt16(3),
                DeliverAt = reader.GetFieldValue<DateTimeOffset>(4),
                CreatedAt = reader.GetFieldValue<DateTimeOffset>(5)
            });
        }

        return results;
    }

    public async Task MarkPromotedAsync(IReadOnlyList<Guid> ids, CancellationToken ct = default)
    {
        if (ids.Count == 0) return;

        const string sql = """
            UPDATE msgflux.scheduled_messages SET state = $1 WHERE message_id = ANY($2)
            """;

        await using var cmd = dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue((short)ScheduledState.Promoted);
        cmd.Parameters.AddWithValue(ids.ToArray());
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<bool> CancelScheduledAsync(Guid id, CancellationToken ct = default)
    {
        const string sql = """
            UPDATE msgflux.scheduled_messages SET state = $1 WHERE message_id = $2 AND state = 0
            """;

        await using var cmd = dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue((short)ScheduledState.Cancelled);
        cmd.Parameters.AddWithValue(id);
        var affected = await cmd.ExecuteNonQueryAsync(ct);
        return affected > 0;
    }

    public async Task<int> PurgeAsync(TimeSpan olderThan, CancellationToken ct = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        // One instance purges per cycle; the rest skip without blocking (auto-released on commit).
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
            DELETE FROM msgflux.scheduled_messages
            WHERE state IN (1, 2) AND created_at < $1
            """, conn, tx);
        cmd.Parameters.AddWithValue(clock.UtcNow - olderThan);
        var deleted = await cmd.ExecuteNonQueryAsync(ct);

        await tx.CommitAsync(ct);
        return deleted;
    }

    private async Task PersistSmallBatchAsync(IReadOnlyList<ScheduledMessage> messages, CancellationToken ct)
    {
        var sb = new StringBuilder(
            "INSERT INTO msgflux.scheduled_messages (message_id, msg, message_type, state, deliver_at, created_at) VALUES ");

        await using var cmd = dataSource.CreateCommand();
        var p = 1;
        for (var i = 0; i < messages.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append('(')
              .Append('$').Append(p++).Append(", ")
              .Append('$').Append(p++).Append(", ")
              .Append('$').Append(p++).Append(", ")
              .Append('$').Append(p++).Append(", ")
              .Append('$').Append(p++).Append(", ")
              .Append('$').Append(p++).Append(')');

            var m = messages[i];
            cmd.Parameters.AddWithValue(m.Id);
            cmd.Parameters.AddWithValue(m.Msg);
            cmd.Parameters.AddWithValue(m.Type);
            cmd.Parameters.AddWithValue((short)m.Status);
            cmd.Parameters.AddWithValue(m.DeliverAt);
            cmd.Parameters.AddWithValue(m.CreatedAt);
        }
        sb.Append(" ON CONFLICT (message_id) DO NOTHING");

        cmd.CommandText = sb.ToString();
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task PersistBulkAsync(IReadOnlyList<ScheduledMessage> messages, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);

        await using var createTemp = new NpgsqlCommand("""
            CREATE TEMP TABLE IF NOT EXISTS _msgflux_scheduled_bulk (
                message_id UUID, msg BYTEA, message_type TEXT,
                state SMALLINT, deliver_at TIMESTAMPTZ, created_at TIMESTAMPTZ
            )
            """, conn);
        await createTemp.ExecuteNonQueryAsync(ct);

        await using var truncate = new NpgsqlCommand("TRUNCATE _msgflux_scheduled_bulk", conn);
        await truncate.ExecuteNonQueryAsync(ct);

        await using var writer = await conn.BeginBinaryImportAsync(
            "COPY _msgflux_scheduled_bulk (message_id, msg, message_type, state, deliver_at, created_at) FROM STDIN (FORMAT BINARY)",
            ct);

        foreach (var m in messages)
        {
            await writer.StartRowAsync(ct);
            await writer.WriteAsync(m.Id, NpgsqlDbType.Uuid, ct);
            await writer.WriteAsync(m.Msg, NpgsqlDbType.Bytea, ct);
            await writer.WriteAsync(m.Type, NpgsqlDbType.Text, ct);
            await writer.WriteAsync((short)m.Status, NpgsqlDbType.Smallint, ct);
            await writer.WriteAsync(m.DeliverAt, NpgsqlDbType.TimestampTz, ct);
            await writer.WriteAsync(m.CreatedAt, NpgsqlDbType.TimestampTz, ct);
        }

        await writer.CompleteAsync(ct);
        await writer.CloseAsync(ct);

        await using var merge = new NpgsqlCommand("""
            INSERT INTO msgflux.scheduled_messages (message_id, msg, message_type, state, deliver_at, created_at)
            SELECT message_id, msg, message_type, state, deliver_at, created_at
            FROM _msgflux_scheduled_bulk
            ON CONFLICT (message_id) DO NOTHING
            """, conn);
        await merge.ExecuteNonQueryAsync(ct);
    }
}
