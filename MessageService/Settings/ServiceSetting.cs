namespace MasterService.Settings;

public class ServiceSetting
{
    [ConfigurationKeyName("DelayInSec")]
    public int Delay
    {
        get;
        set => field = value * 1000;
    }

    public bool IsMaster { get; set; }
    public WriteConcern WriteConcern { get; set; }
    public List<string>? SecondariesEndpoints { get; set; }
}