using Grpc.Core;
using GrpcServices;
using MasterService.Managers;

public class MessageServiceImpl : MessageService.MessageServiceBase
{
    private readonly ClusterManager _clusterManager;
    private readonly ILogger<MessageServiceImpl> _logger;
    
    private static readonly List<MessageResponse> _items = new()
    {
        new MessageResponse { Message = "Test message 1" },
        new MessageResponse { Message = "Test message 2" }
    };

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

    public override Task<MessageResponse> AddMessage(MessageRequest request, ServerCallContext context)
    {
        _logger.LogInformation($"AddMessage called with message: {request.Message}.");

        if (_clusterManager.IsMaster)
        {
            var secondaryTasks = _clusterManager.Clients.Select(client =>
                client.AddMessageAsync(request, cancellationToken: context.CancellationToken).ResponseAsync
            ).ToList();
            Task.WaitAll(secondaryTasks);
        }
        
        var newItem = new MessageResponse
        {
            Message = request.Message
        };
        
        Thread.Sleep(_clusterManager.Delay);
        _items.Add(newItem);
        _logger.LogInformation($"AddMessage finished for message: {request.Message}.");
        return Task.FromResult(newItem);
    }
}