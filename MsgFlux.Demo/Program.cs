using Microsoft.AspNetCore.Mvc;
using MsgFlux.Core;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddOpenApi();
builder.Services.AddLogging(logging =>
{
    logging.AddConsole();
    logging.AddDebug();
});
builder.Services.AddMsgFlux(typeof(Program).Assembly);


var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.MapPost("/orders", async ([FromBody] CreateOrderRequest request, IPublish publisher, ILogger<Program> logger) =>
{
    logger.LogInformation("Received order request for {Product} x {Quantity}", request.Product, request.Quantity);
    var orderCreated = new OrderCreated(Guid.NewGuid(), request.Product, request.Quantity);
    await publisher.PublishAsync(orderCreated);
    logger.LogInformation("Published OrderCreated event for OrderId: {OrderId}", orderCreated.OrderId);
    return Results.Accepted($"/orders/{orderCreated.OrderId}", orderCreated);
})
.WithName("CreateOrder");

app.Run();

// Messages
public record CreateOrderRequest(string Product, int Quantity);
public record OrderCreated(Guid OrderId, string Product, int Quantity);
public record InventoryReserved(Guid OrderId, string Product, int Quantity);

// Consumers
public class OrderCreatedConsumer(IPublish publisher, ILogger<OrderCreatedConsumer> logger) : IConsume<OrderCreated>
{
    public async Task HandleAsync(OrderCreated message, CancellationToken ct)
    {
        logger.LogInformation("Order created: {OrderId} for {Product} x {Quantity}", message.OrderId, message.Product, message.Quantity);
        
        // Simulate processing
        await Task.Delay(100, ct);
        
        // Chain another event
        await publisher.PublishAsync(new InventoryReserved(message.OrderId, message.Product, message.Quantity), ct);
    }
}

public class InventoryReservedConsumer(ILogger<InventoryReservedConsumer> logger) : IConsume<InventoryReserved>
{
    public Task HandleAsync(InventoryReserved message, CancellationToken ct)
    {
        logger.LogInformation("Inventory reserved for Order: {OrderId}", message.OrderId);
        return Task.CompletedTask;
    }
}
