using MasterService.Managers;
using Serilog;


var builder = WebApplication.CreateBuilder(args);

builder.Configuration.AddCommandLine(args);
var customConfigFile = builder.Configuration["configFile"];
builder.Configuration.AddJsonFile(customConfigFile, optional: false, reloadOnChange: true);
builder.Services.AddGrpc();
var url = builder.Configuration["ServiceConfig:SelfUrl"];
bool isMaster = bool.Parse(builder.Configuration["ServiceConfig:IsMaster"] ?? "false");
Console.WriteLine($"Service url: {url}");
Console.WriteLine($"Is master service?: {isMaster}");
builder.Services.AddSingleton<ClusterManager>();

//setup logging

builder.Services.AddSerilog((services, loggerConfiguration) => loggerConfiguration
    .ReadFrom.Configuration(builder.Configuration)
    .ReadFrom.Services(services));

var app = builder.Build();

app.MapGrpcService<MessageServiceImpl>();
app.Run(url);