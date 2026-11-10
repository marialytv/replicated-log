using System.Collections.Concurrent;
using GrpcServices;

namespace MessageService.RetryHelpers;

public class RetryBuffer
{
    // Maps the Secondary URL to a queue of failed requests
    public ConcurrentDictionary<string, ConcurrentQueue<DataRequest>> Queues { get; } = new();

    public void Enqueue(string url, DataRequest request)
    {
        var queue = Queues.GetOrAdd(url, _ => new ConcurrentQueue<DataRequest>());
        queue.Enqueue(request);
    }
}