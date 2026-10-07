namespace MasterService.Settings;

public enum WriteConcern
{ 
    MasterOnly = 1,
    MasterAndSecondary1 = 2,
    All = 3
}