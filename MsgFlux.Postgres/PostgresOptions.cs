namespace MsgFlux.Postgres;

public class PostgresOptions
{
    public bool AutoCreateSchema { get; set; } = true;
    public int BulkInsertThreshold { get; set; } = 5;
}
