using Grpc.Core;
using GrpcServices;
using MessageService.Managers;
using MessageService.ReplicationHelpers;
using MessageService.RetryHelpers;
using MessageService.Settings;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace MessageService.Services;

public class MessageServiceImpl : GrpcServices.MessageService.MessageServiceBase
{
    private readonly ClusterManager _clusterManager;
    private readonly ILogger<MessageServiceImpl> _logger;
    private static readonly List<MessageResponse> _items = new();
    private readonly WriteAheadLog _wal;
    private readonly SequenceGenerator _sequenceGenerator;
    private readonly RetryBuffer _retryBuffer;

    public MessageServiceImpl(ClusterManager clusterManager, WriteAheadLog wal, SequenceGenerator sequenceGenerator,
        RetryBuffer retryBuffer,
        ILogger<MessageServiceImpl> logger)
    {
        _clusterManager = clusterManager;
        _wal = wal;
        _sequenceGenerator = sequenceGenerator;
        _retryBuffer = retryBuffer;
        _logger = logger;
    }

    public override Task<HealthResponse> CheckHealth(Empty request, ServerCallContext context)
    {
        _logger.Log(LogLevel.Information, "CheckHealth called.");
        return Task.FromResult(!_clusterManager.ServiceSetting.IsAvailable
            ? new HealthResponse { Status = (int)HealthStatus.Unhealthy }
            : new HealthResponse { Status = (int)HealthStatus.Healthy });
    }

    public override Task<MessageList> GetMessages(Empty request, ServerCallContext context)
    {
        var list = new MessageList();
        if (_clusterManager.ServiceSetting.IsMaster)
        {
            list.Items.AddRange(_items);
        }
        else
        {
            var committedMessages = _wal.GetCommittedMessages();
            list.Items.AddRange(committedMessages.Select(x => new MessageResponse { Message = x }));
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

        _logger.LogInformation(
            $"AddMessage called with message: {request.Message}, writeConcern={request.WriteConcern}.");
        var newItem = new MessageResponse
        {
            Message = request.Message
        };
        _items.Add(newItem);

        long sequenceId = _sequenceGenerator.GetNext();
        var downstreamReq = new DataRequest
        {
            Message = request.Message,
            SequenceID = sequenceId
        };
        var targetAcks = request.WriteConcern switch
        {
            (int)WriteConcern.MasterAndSecondary => 1,
            (int)WriteConcern.All => _clusterManager.Clients.Count,
            (int)WriteConcern.MasterOnly => 0
        };


        var pendingNodes = _clusterManager.Clients.ToDictionary(k => k.Key, v => v.Value);

        // =========================================================
        // PATH 1: MASTER ONLY (Fire-and-Forget, No Blocking)
        // =========================================================
        if (targetAcks == 0)
        {
            // Detach entirely from the gRPC request lifecycle
            _ = Task.Run(async () =>
            {
                foreach (var client in pendingNodes)
                {
                    try
                    {
                        await client.Value.ProcessMessageAsync(downstreamReq);
                    }
                    catch
                    {
                        _retryBuffer.Enqueue(client.Key, downstreamReq);
                    }
                }
            });

            return await Task.FromResult(newItem);
        }

        // =========================================================
        // PATH 2: BLOCKING WRITE CONCERNS (Wait for Any/All)
        // =========================================================
        bool isFirstAttempt = true;

        // 1. Create a hard 10-second timeout linked to the client's connection state
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(context.CancellationToken);
        timeoutCts.CancelAfter(_clusterManager.ServiceSetting.Timeout);

        async Task<(string Url, bool Success)> TryDeliverWithRetriesAsync(string url, GrpcServices.MessageService.MessageServiceClient client)
        {
            bool isFirstAttempt = true;
        
            while (!timeoutCts.Token.IsCancellationRequested)
            {
                try
                {
                    // 1. Health check (Skip on the very first try)
                    if (!isFirstAttempt)
                    {
                        var health = await client.CheckHealthAsync(new Empty(), deadline: DateTime.UtcNow.AddSeconds(1));
                        if (health.Status != (int)HealthStatus.Healthy)
                        {
                            await Task.Delay(_clusterManager.ServiceSetting.RetryInterval, timeoutCts.Token); // Wait and loop again
                            continue;
                        }
                    }
                    
                    await client.ProcessMessageAsync(downstreamReq, cancellationToken: CancellationToken.None).ResponseAsync;
                    return (url, true); // Success!
                }
                catch (RpcException)
                {
                    // 3. Network failed. Wait 2 seconds before checking health again.
                    isFirstAttempt = false;
                    try
                    {
                        await Task.Delay(_clusterManager.ServiceSetting.RetryInterval, timeoutCts.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            }

            // 4. Timeout Reached. Secondary is still dead. Hand to background worker.
            _retryBuffer.Enqueue(url, downstreamReq);
            return (url, false);
        }
        
        // Launch the retry loop for every secondary simultaneously
        var deliveryTasks = _clusterManager.Clients
            .Select(kvp => TryDeliverWithRetriesAsync(kvp.Key, kvp.Value))
            .ToList();

        int currentAcks = 0;

        // Track completions as they happen
        while (deliveryTasks.Any() && currentAcks < targetAcks)
        {
            var completedTask = await Task.WhenAny(deliveryTasks);
            deliveryTasks.Remove(completedTask);
        
            var result = await completedTask;
            if (result.Success)
            {
                currentAcks++;
            }
        }

        // Once we exit the loop, cancel any tasks that are STILL running 
        // (e.g., if target was 1, but 2 are still polling). Canceling them forces 
        // them to drop their payload into the _retryBuffer!
        timeoutCts.Cancel(); 

        // Evaluate final success
        if (currentAcks < targetAcks)
        {
            string reason = context.CancellationToken.IsCancellationRequested 
                ? "Client disconnected." 
                : "Replication timeout (10s) exceeded.";

            throw new RpcException(new Status(StatusCode.Unavailable, 
                $"Write Concern not met. {reason} Background syncing continues."));
        }

        _logger.LogInformation($"AddMessage finished for message: {request.Message}.");
        return newItem;
        
    }

    public override async Task<DataResponse> ProcessMessage(DataRequest request, ServerCallContext context)
    {
        if (_clusterManager.ServiceSetting.IsMaster)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied,
                "Only Secondary nodes process replication data."));
        }

        if (!_clusterManager.ServiceSetting.IsAvailable)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, "Service is unavailable."));
        }

        //introduce a delay to emulate messages reordering
        if (_clusterManager.ServiceSetting.IsReorderingEnabled)
        {
            var randomDelay = Random.Shared.Next(10, 500);
            await Task.Delay(randomDelay);
        }

        _logger.LogInformation($"ProcessMessage called with message: {request.Message}, seqID={request.SequenceID}.");
        Thread.Sleep(_clusterManager.ServiceSetting.Delay);

        var result = await _wal.AppendAsync(request.SequenceID, request.Message);

        _logger.LogInformation($"ProcessMessage finished for message: {request.Message}, seqID={request.SequenceID}.");
        return new DataResponse { Output = result };
    }
    
}