using MasterService.Managers;
using MasterService.Settings;
using Serilog;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddGrpc();
var url = builder.Configuration["ServiceConfig:SelfUrl"];

builder.Services.Configure<ServiceSetting>(
    builder.Configuration.GetSection("ServiceConfig"));
builder.Services.AddSingleton<ClusterManager>();

//setup logging

builder.Services.AddSerilog((services, loggerConfiguration) => loggerConfiguration
    .ReadFrom.Configuration(builder.Configuration)
    .ReadFrom.Services(services));

var app = builder.Build();

app.MapGrpcService<MessageServiceImpl>();
app.Run(url);

public partial class Program { }