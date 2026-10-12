namespace MasterService.Settings;

public enum WriteConcern
{ 
    MasterOnly = 1,
    MasterAndSecondary = 2,
    All = 3
}