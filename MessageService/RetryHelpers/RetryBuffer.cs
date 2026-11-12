using System.Collections.Concurrent;
using GrpcServices;

namespace MessageService.RetryHelpers;

public class RetryBuffer
{
    // A standard FIFO queue per node URL
    private readonly ConcurrentDictionary<string, ConcurrentQueue<DataRequest>> _queues = new();

    public void Enqueue(string nodeUrl, DataRequest request)
    {
        var queue = _queues.GetOrAdd(nodeUrl, _ => new ConcurrentQueue<DataRequest>());
        queue.Enqueue(request);
    }

    public bool TryPeek(string nodeUrl, out DataRequest? request)
    {
        if (_queues.TryGetValue(nodeUrl, out var queue))
        {
            return queue.TryPeek(out request);
        }
        
        request = null;
        return false;
    }

    public bool TryDequeue(string nodeUrl, out DataRequest? request)
    {
        if (_queues.TryGetValue(nodeUrl, out var queue))
        {
            return queue.TryDequeue(out request);
        }
        
        request = null;
        return false;
    }

    public IEnumerable<string> GetNodesWithPendingMessages()
    {
        return _queues.Where(kvp => !kvp.Value.IsEmpty).Select(kvp => kvp.Key).ToList();
    }
}