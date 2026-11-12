using MessageService.Managers;
using MessageService.ReplicationHelpers;
using MessageService.RetryHelpers;
using MessageService.Services;
using MessageService.Settings;
using Serilog;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddGrpc();
var url = builder.Configuration["ServiceConfig:SelfUrl"];

builder.Services.Configure<ServiceSetting>(
    builder.Configuration.GetSection("ServiceConfig"));
builder.Services.AddSingleton<ClusterManager>();
builder.Services.AddSingleton<WriteAheadLog>();
builder.Services.AddSingleton<SequenceGenerator>();
builder.Services.AddSingleton<RetryBuffer>();
if (builder.Configuration.GetValue<bool>("ServiceConfig:IsMaster"))
{
    builder.Services.AddHostedService<RetryBackgroundWorker>();
}
//setup logging

builder.Services.AddSerilog((services, loggerConfiguration) => loggerConfiguration
    .ReadFrom.Configuration(builder.Configuration)
    .ReadFrom.Services(services));

var app = builder.Build();

app.MapGrpcService<MessageServiceImpl>();
app.Run(url);

namespace MessageService
{
    public partial class Program
    {
    }
}
