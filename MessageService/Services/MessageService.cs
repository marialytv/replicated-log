using Grpc.Core;
using GrpcServices;
using MasterService.Managers;
using MasterService.Settings;

public class MessageServiceImpl : MessageService.MessageServiceBase
{
    private readonly ClusterManager _clusterManager;
    private readonly ILogger<MessageServiceImpl> _logger;

    private static readonly List<MessageResponse> _items = new();

    public MessageServiceImpl(ClusterManager clusterManager, ILogger<MessageServiceImpl> logger)
    {
        _clusterManager = clusterManager;
        _logger = logger;
    }

    public override Task<MessageList> GetMessages(Empty request, ServerCallContext context)
    {
        _logger.LogInformation($"GetMessages called.");
        var list = new MessageList();
        list.Items.AddRange(_items);
        return Task.FromResult(list);
        ;
    }

    public override async Task<MessageResponse> AddMessage(MessageRequest request, ServerCallContext context)
    {
        _logger.LogInformation($"AddMessage called with message: {request.Message}.");
        Thread.Sleep(_clusterManager.ServiceSetting.Delay);
        var newItem = new MessageResponse
        {
            Message = request.Message
        };
        _items.Add(newItem);
        if (_clusterManager.ServiceSetting.IsMaster)
        {
            //start replication regardless of writeconcern value
            var secondaryTasks = _clusterManager.Clients.Select(client =>
                client.AddMessageAsync(request).ResponseAsync
            ).ToList();
            //wait for all secondaries to finish replication
            if (request.WriteConcern == (int)WriteConcern.All)
            {
                await Task.WhenAll(secondaryTasks);
            }
            //wait for at least one secondary to finish replication
            else if (request.WriteConcern == (int)WriteConcern.MasterAndSecondary)
            {
                await Task.WhenAny(secondaryTasks);
            }
        }


        _logger.LogInformation($"AddMessage finished for message: {request.Message}.");
        return await Task.FromResult(newItem);
    }
}