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
    }

    public override async Task<MessageResponse> AddMessage(MessageRequest request, ServerCallContext context)
    {
        _logger.LogInformation($"AddMessage called with message: {request.Message}.");

        if (_clusterManager.ServiceSetting.IsMaster)
        {
            _logger.LogInformation($"Write concern is: {_clusterManager.ServiceSetting.WriteConcern}.");
            
            //start replication regardless of set writeconcern parameter
            var secondaryTasks = _clusterManager.Clients.Select(client =>
                client.AddMessageAsync(request, cancellationToken: context.CancellationToken).ResponseAsync
            ).ToList();
            if (_clusterManager.ServiceSetting.WriteConcern == WriteConcern.All)
            {
                Task.WaitAll(secondaryTasks);
            }
            else if (_clusterManager.ServiceSetting.WriteConcern == WriteConcern.MasterAndSecondary1)
            {
                await secondaryTasks[0];
            }
        }
        
        var newItem = new MessageResponse
        {
            Message = request.Message
        };
        
        Thread.Sleep(_clusterManager.ServiceSetting.Delay);
        _items.Add(newItem);
        _logger.LogInformation($"AddMessage finished for message: {request.Message}.");
        return await Task.FromResult(newItem);
    }
}