namespace MessageService.ReplicationHelpers;

public class WriteAheadLog
{
    private long _expectedSequence = 1;

    // buffer for unordered messages
    private readonly SortedDictionary<long, string> _buffer = new();

    //ordered messages
    private readonly List<string> _committedMessages = [];

    private readonly Lock _lock = new();

    public Task<string> AppendAsync(long sequenceId, string message)
    {
        lock (_lock)
        {
            if (sequenceId < _expectedSequence)
            {
                return Task.FromResult("Ignored (Duplicate)");
            }

            if (sequenceId > _expectedSequence)
            {
                _buffer[sequenceId] = message;
                return Task.FromResult("Buffered Out of Order");
            }

            Commit(sequenceId, message);
            _expectedSequence++;

            while (_buffer.TryGetValue(_expectedSequence, out var bufferedMessage))
            {
                Commit(_expectedSequence, bufferedMessage);
                _buffer.Remove(_expectedSequence);
                _expectedSequence++;
            }

            return Task.FromResult($"Processed {message} and Committed");
        }
    }

    private void Commit(long sequenceId, string message)
    {
        _committedMessages.Add(message);
    }

    public IReadOnlyList<string> GetCommittedMessages()
    {
        lock (_lock)
        {
            return new List<string>(_committedMessages);
        }
    }
}