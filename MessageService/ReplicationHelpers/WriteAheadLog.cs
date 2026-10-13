namespace MasterService.ReplicationHelpers;

public class WriteAheadLog
{
    private long _expectedSequence = 1;

    // buffer for unordered messages
    private readonly Dictionary<long, PendingMessage> _buffer = new();

    //ordered messages
    private readonly List<string> _committedMessages = [];

    private readonly Lock _lock = new();

    public Task<string> AppendAsync(long sequenceId, string input)
    {
        var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var request = new PendingMessage(sequenceId, input, tcs);
        var readyToCommit = new List<PendingMessage>();

        lock (_lock)
        {
            if (sequenceId < _expectedSequence)
            {
                tcs.SetResult($"Message {input} ignored (Old/Duplicate)");
                return tcs.Task;
            }

            _buffer[sequenceId] = request;

            //check if buffer already has sequentially messages and append them to _committedLog
            while (_buffer.TryGetValue(_expectedSequence, out var ready))
            {
                _buffer.Remove(_expectedSequence);

                // Append to the ordered in-memory log
                _committedMessages.Add(ready.Message);

                readyToCommit.Add(ready);
                _expectedSequence++;
            }
        }

        // 4. Resolve the Tasks OUTSIDE the lock to prevent blocking other threads
        foreach (var item in readyToCommit)
        {
            item.CompletionSource.SetResult($"Committed Sequence {item.SequenceId} to memory");
        }

        return tcs.Task;
    }

    public IReadOnlyList<string> GetCommittedMessages()
    {
        lock (_lock)
        {
            return new List<string>(_committedMessages);
        }
    }

    private record PendingMessage(long SequenceId, string Message, TaskCompletionSource<string> CompletionSource);
}