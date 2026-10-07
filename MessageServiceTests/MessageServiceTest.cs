namespace MessageServiceTests;

using MasterService.Settings;
using System.Diagnostics;
using Grpc.Net.Client;
using GrpcServices;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using NUnit.Framework;

[TestFixture]
[NonParallelizable]
public class MessageServiceTest
{
    private KestrelWebApplicationFactory<Program>? _secondary1Factory;
    private KestrelWebApplicationFactory<Program>? _secondary2Factory;
    private WebApplicationFactory<Program>? _masterFactory;
    
    public void Setup(WriteConcern writeConcern, int secondary1Delay, int secondary2Delay)
    {
        _secondary1Factory = new KestrelWebApplicationFactory<Program>(5001, new Dictionary<string, string>
        {
            { "ServiceConfig:DelayInSec", secondary1Delay.ToString() },
            { "ServiceConfig:IsMaster", "false" },
            { "ServiceConfig:SelfUrl", "http://localhost:5001"}
        });
        _secondary2Factory = new KestrelWebApplicationFactory<Program>(5002, new Dictionary<string, string>
        {
            { "ServiceConfig:DelayInSec", secondary2Delay.ToString() },
            { "ServiceConfig:IsMaster", "false" },
            { "ServiceConfig:SelfUrl", "http://localhost:5002"}
        });
        _secondary1Factory.Boot();
        _secondary2Factory.Boot();
        
        _masterFactory = new WebApplicationFactory<Program>()
            .WithWebHostBuilder(builder =>
            {
                builder.ConfigureAppConfiguration((context, config) =>
                {
                    var configs = new Dictionary<string, string>
                    {
                        { "ServiceConfig:DelayInSec", "0" },
                        { "ServiceConfig:IsMaster", "true" },
                        { "ServiceConfig:WriteConcern", ((int)writeConcern).ToString() },
                        { "ServiceConfig:SelfUrl", "http://localhost:5003" },
                        { "ServiceConfig:SecondariesEndpoints:0", "http://localhost:5001" },
                        { "ServiceConfig:SecondariesEndpoints:1", "http://localhost:5002" }
                    };
                    config.AddInMemoryCollection(configs!);
                });
            });
    }
    
    [TearDown]
    public void TearDown()
    {
        _masterFactory?.Dispose();
        _secondary1Factory?.RealKestrelHost?.StopAsync().Wait();
        _secondary1Factory?.Dispose();
        _secondary2Factory?.RealKestrelHost?.StopAsync().Wait();
        _secondary2Factory?.Dispose();
    }

    [Test]
    [TestCase(WriteConcern.MasterOnly, 2,5,0)]
    [TestCase(WriteConcern.All, 2,5,5)]
    [TestCase(WriteConcern.All, 7,5,7)]
    [TestCase(WriteConcern.MasterAndSecondary1, 2,5,2)]
    [TestCase(WriteConcern.MasterAndSecondary1, 2,7,2)]
    [TestCase(WriteConcern.MasterAndSecondary1, 0,3,0)]
    public void Test(WriteConcern writeConcern, int secondary1Delay, int secondary2Delay, int expected)
    {
        Setup(writeConcern, secondary1Delay, secondary2Delay);
        
        var httpClient = _masterFactory!.CreateDefaultClient();
        var masterChannel = GrpcChannel.ForAddress("http://localhost:5003", new GrpcChannelOptions
        {
            HttpClient = httpClient
        });
        
        var masterClient = new MessageService.MessageServiceClient(masterChannel);
        
        var stopwatch = new Stopwatch();
        stopwatch.Start();
        masterClient.AddMessage(new MessageRequest{Message = "msg1"});
        stopwatch.Stop();
        
        Assert.That(stopwatch.Elapsed.Seconds, Is.EqualTo(expected));
    }
}