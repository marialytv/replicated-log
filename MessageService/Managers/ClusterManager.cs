using Grpc.Net.Client;
using GrpcServices;
namespace MasterService.Managers;

public class ClusterManager
{
    public bool IsMaster { get;  }
    /// <summary>
    /// Service delay in ms
    /// </summary>
    public int Delay { get; } 
    
    private readonly List<GrpcChannel> _channels = new();
    public List<MessageService.MessageServiceClient> Clients { get; } = new();

    public ClusterManager(IConfiguration config)
    {
        IsMaster = config.GetValue<bool>("ServiceConfig:IsMaster");
        Delay = config.GetValue<int>("ServiceConfig:DelayInSec") * 1000;
        
        var endpoints = config.GetSection("ServiceConfig:SecondariesEndpoints").Get<string[]>() ?? Array.Empty<string>();

        foreach (var url in endpoints)
        {
            var channel = GrpcChannel.ForAddress(url);
            _channels.Add(channel);
            Clients.Add(new MessageService.MessageServiceClient(channel));
        }
    }

    public void Dispose()
    {
        foreach (var channel in _channels)
        {
            channel.Dispose();
        }
    }
}