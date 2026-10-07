using Grpc.Net.Client;
using Grpc.Net.ClientFactory;
using GrpcServices;
using MasterService.Settings;
using Microsoft.Extensions.Options;

namespace MasterService.Managers;

public class ClusterManager
{
    private readonly IDisposable _changeListener;
    private readonly List<GrpcChannel> _channels = new();
    private readonly ILogger<ClusterManager> _logger;
    public ServiceSetting ServiceSetting { get; private set; }
    public List<MessageService.MessageServiceClient> Clients { get; } = new();

    public ClusterManager(IOptionsMonitor<ServiceSetting> optionsMonitor, ILogger<ClusterManager> logger)
    {
        _logger = logger;
        ServiceSetting = optionsMonitor.CurrentValue;
        _changeListener = optionsMonitor.OnChange(newSettings =>
        {
            ServiceSetting = newSettings;
            _logger.LogInformation($"Service config was changed. New configs are: " +
                                   $"\nDelay = {TimeSpan.FromMilliseconds(ServiceSetting.Delay).Seconds}s." +
                                   $"Write Concern = {ServiceSetting.WriteConcern.ToString()}(value={(int)ServiceSetting.WriteConcern}).");
        })!;
        
        if (ServiceSetting.IsMaster)
        { 
            CreateClients();
        }
    }
    private void CreateClients()
    {
        foreach (var url in ServiceSetting.SecondariesEndpoints ?? Enumerable.Empty<string>())
        {
            var channel = GrpcChannel.ForAddress(url);
            _channels.Add(channel);
            Clients.Add(new MessageService.MessageServiceClient(channel));
        }
    }
    
    public void Dispose()
    {
        _changeListener.Dispose();
        foreach (var channel in _channels)
        {
            channel.Dispose();
        }
    }
}