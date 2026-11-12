using Grpc.Core;
using GrpcServices;
using MessageService.Managers;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace MessageService.RetryHelpers;

public class RetryBackgroundWorker : BackgroundService
{
    private readonly RetryBuffer _retryBuffer;
    private readonly ClusterManager _clusterManager;
    private readonly ILogger<RetryBackgroundWorker> _logger;

    public RetryBackgroundWorker(RetryBuffer retryBuffer, ClusterManager clusterManager,
        ILogger<RetryBackgroundWorker> logger)
    {
        _retryBuffer = retryBuffer;
        _clusterManager = clusterManager;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Retry Background Worker started.");

        // Run continuously until the ASP.NET Core application shuts down
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            { 
                await ProcessRetriesAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error occurred during the background retry cycle.");
            }

            try
            {
                await Task.Delay(_clusterManager.ServiceSetting.RetryInterval, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break; // Gracefully exit if stoppingToken triggers during the delay
            }
        }
    }

    private async Task ProcessRetriesAsync(CancellationToken stoppingToken)
    {
        // Fetch only the URLs that currently have messages in their queues
        var nodesToRetry = _retryBuffer.GetNodesWithPendingMessages();

        foreach (var url in nodesToRetry)
        {
            if (stoppingToken.IsCancellationRequested) return;

            if (!_clusterManager.Clients.TryGetValue(url, out var client))
            {
                continue;
            }

            bool isHealthy = false;
            try
            {
                // 1. Check if the secondary is back online
                var healthResponse = await client.CheckHealthAsync(
                    new Empty(), deadline: DateTime.UtcNow.AddSeconds(2));

                isHealthy = healthResponse.Status == (int)HealthStatus.Healthy;
            }
            catch (RpcException)
            {
                // Node is still unreachable. Leave messages in the queue and skip to the next node.
                continue;
            }

            // 2. Drain the queue sequentially if the node is healthy
            if (isHealthy)
            {
                
                while (_retryBuffer.TryPeek(url, out var pendingReq))
                {
                    if (stoppingToken.IsCancellationRequested) return;

                    try
                    {
                        await client.ProcessMessageAsync(pendingReq);

                        _retryBuffer.TryDequeue(url, out _);
                        _logger.LogInformation(
                            $"BACKGROUND RECOVERY: Delivered missed Sequence {pendingReq.SequenceID} to {url}.");
                    }
                    catch (RpcException ex)
                    {
                        // The node crashed or network failed MID-drain. 
                        // Break the inner loop. The message remains safely in the queue.
                        _logger.LogWarning($"Node {url} failed mid-drain. Halting. Error: {ex.Status.Detail}");
                        break;
                    }
                }
            }
        }
    }
}