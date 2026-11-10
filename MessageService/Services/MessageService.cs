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
        int currentAcks = 0;
        bool isFirstAttempt = true;

        // 1. Create a hard 10-second timeout linked to the client's connection state
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(context.CancellationToken);
        timeoutCts.CancelAfter(_clusterManager.ServiceSetting.Timeout);

        try
        {
            while (currentAcks < targetAcks)
            {
                var attemptTasks = pendingNodes.Select(async node =>
                {
                    var url = node.Key;
                    var client = node.Value;
                    try
                    {
                        if (!isFirstAttempt)
                        {
                            var health = await client.CheckHealthAsync(new Empty());
                            if (health.Status != (int)HealthStatus.Healthy) return (Url: url, Success: false);
                        }

                        //need to set a deadline,because grpc will handle connection in case call throws an exception
                        await client.ProcessMessageAsync(downstreamReq, deadline: DateTime.UtcNow.AddSeconds(2))
                            .ResponseAsync;
                        return (Url: url, Success: true);
                    }
                    catch
                    {
                        return (Url: url, Success: false);
                    }
                }).ToList();

                while (attemptTasks.Any() && currentAcks < targetAcks)
                {
                    var timeoutTask = Task.Delay(Timeout.Infinite, timeoutCts.Token);
                    // Wait for a secondary to reply OR for the 10-second timeout to trip
                    var completedTask = await Task.WhenAny(attemptTasks.Cast<Task>().Append(timeoutTask));

                    if (completedTask == timeoutTask)
                    {
                        break; // The timeout fuse blew. Break out of the inner loop immediately.
                    }

                    // Extract the result from the secondary
                    var actualTask = (Task<(string Url, bool Success)>)completedTask;
                    attemptTasks.Remove(actualTask);
                    var res = await actualTask;

                    if (res.Success)
                    {
                        pendingNodes.Remove(res.Url);
                        currentAcks++;
                    }
                }

                // 3. Exit Conditions
                if (currentAcks >= targetAcks) break; // Success!
                if (timeoutCts.Token.IsCancellationRequested) break; // Timeout!

                isFirstAttempt = false;

                await Task.Delay(_clusterManager.ServiceSetting.RetryInterval, timeoutCts.Token);
            }
        }
        catch (OperationCanceledException)
        {
            // This gracefully catches the TaskCanceledException thrown if 
            // Task.Delay(2000, timeoutCts.Token) is forcefully interrupted by the timeout.
        }

        // 4. Handle remaining slow or broken nodes
        if (pendingNodes.Any())
        {
            foreach (var brokenClient in pendingNodes)
            {
                _retryBuffer.Enqueue(brokenClient.Key, downstreamReq);
            }
        }

        // 5. Evaluate final status and report to the client
        if (currentAcks < targetAcks)
        {
            // Determine exactly why we failed to inform the client accurately
            string failureReason = context.CancellationToken.IsCancellationRequested
                ? "Client disconnected before write concern was met."
                : $"Replication timeout ({TimeSpan.FromMilliseconds(_clusterManager.ServiceSetting.Timeout).Seconds}s) exceeded.";

            throw new RpcException(new Status(StatusCode.Unavailable,
                $"Write Concern not met. {failureReason} Background syncing will continue."));
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