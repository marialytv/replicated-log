using Grpc.Core;
using GrpcServices;
using MasterService.Managers;
using MasterService.ReplicationHelpers;
using MasterService.Settings;

public class MessageServiceImpl : MessageService.MessageServiceBase
{
    private readonly ClusterManager _clusterManager;
    private readonly ILogger<MessageServiceImpl> _logger;
    private static readonly List<MessageResponse> _items = new();
    private readonly WriteAheadLog _wal;
    private readonly SequenceGenerator _sequenceGenerator;

    public MessageServiceImpl(ClusterManager clusterManager, WriteAheadLog wal, SequenceGenerator sequenceGenerator, ILogger<MessageServiceImpl> logger)
    {
        _clusterManager = clusterManager;
        _wal = wal;
        _sequenceGenerator = sequenceGenerator;
        _logger = logger;
    }

    public override Task<MessageList> GetMessages(Empty request, ServerCallContext context)
    {
        _logger.LogInformation($"GetMessages called.");
        var list = new MessageList();
        if (_clusterManager.ServiceSetting.IsMaster)
        {
            list.Items.AddRange(_items);
        }
        else
        {
            var snapshot = _wal.GetCommittedMessages();
            // _logger.LogInformation(string.Join(", ", snapshot));
            
            list.Items.AddRange(snapshot.Select(x => new MessageResponse { Message = x }));
        }

        return Task.FromResult(list);
    }

    public override async Task<MessageResponse> AddMessage(MessageRequest request, ServerCallContext context)
    {
        if (!_clusterManager.ServiceSetting.IsMaster)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied,
                "AddMessage cannot be called on any other node, except from Master."));
        }

        _logger.LogInformation($"AddMessage called with message: {request.Message}.");
        var newItem = new MessageResponse
        {
            Message = request.Message
        };
        _items.Add(newItem);
        
        long sequenceId = _sequenceGenerator.GetNext();
        _logger.LogInformation($"{sequenceId}, {request.Message}");
        var downstreamReq = new DataRequest
        {
            Message = request.Message,
            SequenceID = sequenceId
        };

        //start replication regardless of writeconcern value
        var secondaryTasks = _clusterManager.Clients.Select(client =>
            client.ProcessMessageAsync(downstreamReq).ResponseAsync
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

        _logger.LogInformation($"AddMessage finished for message: {request.Message}.");
        return await Task.FromResult(newItem);
    }

    public override async Task<DataResponse> ProcessMessage(DataRequest request, ServerCallContext context)
    {
        if (_clusterManager.ServiceSetting.IsMaster)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied,
                "Only Secondary nodes process replication data."));
        }

        _logger.LogInformation($"ProcessMessage called with message: {request.Message}.");
        Thread.Sleep(_clusterManager.ServiceSetting.Delay);

        var result = await _wal.AppendAsync(request.SequenceID, request.Message);

        _logger.LogInformation($"ProcessMessage finished for message: {request.Message}.");
        return new DataResponse { Output = result };
    }
}