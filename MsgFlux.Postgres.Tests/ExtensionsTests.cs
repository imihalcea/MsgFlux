using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MsgFlux.Abstractions;
using Npgsql;

namespace MsgFlux.Postgres.Tests;

public class ExtensionsTests
{
    [Test]
    public void AddMsgFluxPostgres_Should_Register_All_Services()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMsgFluxPostgres(PostgresFixture.ConnectionString);

        var provider = services.BuildServiceProvider();

        Assert.That(provider.GetService<IMessageStore>(), Is.Not.Null);
        Assert.That(provider.GetService<IMessageStore>(), Is.InstanceOf<PostgresMessageStore>());
        Assert.That(provider.GetService<NpgsqlDataSource>(), Is.Not.Null);
        Assert.That(provider.GetService<PostgresOptions>(), Is.Not.Null);
    }

    [Test]
    public void AddMsgFluxPostgres_Should_Apply_Custom_Options()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMsgFluxPostgres(PostgresFixture.ConnectionString, opts =>
        {
            opts.AutoCreateSchema = false;
        });

        var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<PostgresOptions>();

        Assert.That(options.AutoCreateSchema, Is.False);
    }
}
