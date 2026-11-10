namespace MessageService.Settings;

public class ServiceSetting
{
    [ConfigurationKeyName("DelayInSec")]
    public int Delay
    {
        get;
        set => field = value * 1000;
    }

    public bool IsReorderingEnabled { get; set; } = false;
    public bool IsAvailable { get; set; } = true;

    [ConfigurationKeyName("TimeoutInSec")]
    public int Timeout
    {
        get;
        set => field = value * 1000;
    } = 10000; //default timeout is 10s

    [ConfigurationKeyName("RetryIntervalInSec")]
    public int RetryInterval
    {
        get;
        set => field = value * 1000;
    } = 2000; //default retry interval is 2s

    public bool IsMaster { get; set; }
    public List<string>? SecondariesEndpoints { get; set; }
}