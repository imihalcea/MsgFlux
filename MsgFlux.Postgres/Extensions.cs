using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using MsgFlux.Abstractions;
using Npgsql;

namespace MsgFlux.Postgres;

public static class Extensions
{
    public static IServiceCollection AddMsgFluxPostgres(
        this IServiceCollection services,
        string connectionString,
        Action<PostgresOptions>? configure = null)
    {
        var options = new PostgresOptions();
        configure?.Invoke(options);
        services.AddSingleton(options);

        var dataSource = NpgsqlDataSource.Create(connectionString);
        services.AddSingleton(dataSource);

        services.TryAddSingleton<IClock, SystemClock>();
        services.AddSingleton<IMessageStore, PostgresMessageStore>();
        services.AddHostedService<SchemaInitializer>();

        return services;
    }
}
