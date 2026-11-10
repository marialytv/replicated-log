using GrpcServices;
using MessageService.Managers;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace MessageService.RetryHelpers;

public class RetryBackgroundWorker : BackgroundService
{
    private readonly RetryBuffer _retryBuffer;
    private readonly ClusterManager _clusterManager;

    public RetryBackgroundWorker(RetryBuffer retryBuffer, ClusterManager clusterManager)
    {
        _retryBuffer = retryBuffer;
        _clusterManager = clusterManager;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            foreach (var kvp in _retryBuffer.Queues)
            {
                var url = kvp.Key;
                var queue = kvp.Value;

                if (queue.IsEmpty) continue;

                if (_clusterManager.Clients.TryGetValue(url, out var client))
                {
                    try
                    {
                        // 1. Check health
                        var health = await client.CheckHealthAsync(new Empty(), deadline: DateTime.UtcNow.AddSeconds(2));
                        
                        // 2. If Healthy, drain the queue
                        if (health.Status == (int)HealthStatus.Healthy)
                        {
                            // Peek and process so we don't lose the message if it fails mid-drain
                            while (queue.TryPeek(out var req))
                            {
                                await client.ProcessMessageAsync(req);
                                queue.TryDequeue(out _); // Remove only after successful send
                            }
                        }
                    }
                    catch
                    {
                        // Secondary is still down, or failed mid-drain. Leave items in queue.
                    }
                }
            }
            
            await Task.Delay(_clusterManager.ServiceSetting.RetryInterval, stoppingToken);
        }
    }
}