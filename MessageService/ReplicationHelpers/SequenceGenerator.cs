namespace MasterService.ReplicationHelpers;

public class SequenceGenerator
{
    private long _currentSequence = 0;

    public long GetNext()
    {
        return Interlocked.Increment(ref _currentSequence);
    }
}