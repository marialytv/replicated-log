using Grpc.Net.Client;
using GrpcServices;

var masterAddress = Environment.GetEnvironmentVariable("master-node") ?? "http://localhost:5003";
using var masterChannel = GrpcChannel.ForAddress(masterAddress);
var masterClient = new MessageService.MessageServiceClient(masterChannel);

var secondary1Address = Environment.GetEnvironmentVariable("secondary1-node") ?? "http://localhost:5001";
using var secondary1Channel = GrpcChannel.ForAddress(secondary1Address);
var secondary1Client = new MessageService.MessageServiceClient(secondary1Channel);

var secondary2Address = Environment.GetEnvironmentVariable("secondary2-node") ?? "http://localhost:5002";
using var secondary2Channel = GrpcChannel.ForAddress(secondary2Address);
var secondary2Client = new MessageService.MessageServiceClient(secondary2Channel);

Console.WriteLine(" Console application started.");

var task1 = await masterClient.AddMessageAsync(new MessageRequest { Message = "msg1", WriteConcern = 1 }).ResponseAsync;
RunGetRequests();
var task2 = await masterClient.AddMessageAsync(new MessageRequest { Message = "msg2", WriteConcern = 2 }).ResponseAsync;
RunGetRequests();
var task3 =  masterClient.AddMessageAsync(new MessageRequest { Message = "msg3", WriteConcern = 3 }).ResponseAsync;
RunGetRequests();
var task4 = await masterClient.AddMessageAsync(new MessageRequest { Message = "msg4", WriteConcern = 1 }).ResponseAsync;
RunGetRequests();
int secondsLeft = 8;
while (secondsLeft > 0)
{
    Console.Write($"\rCall another Get requests to check the final state in: {secondsLeft} seconds   ");

    Thread.Sleep(1000);
    secondsLeft--;
}

Console.WriteLine();
RunGetRequests();

void RunGetRequests()
{
    Console.Write("Messages from master: ");
    var updatedMasterItems = masterClient.GetMessages(new Empty());
    WriteMessages(updatedMasterItems);
    Console.Write("Messages from secondary1: ");
    var updatedSecondary1Items = secondary1Client.GetMessages(new Empty());
    WriteMessages(updatedSecondary1Items);
    Console.Write("Messages from secondary2: ");
    var updatedSecondary2Items = secondary2Client.GetMessages(new Empty());
    WriteMessages(updatedSecondary2Items);
}

void WriteMessages(MessageList messages)
{
    foreach (var item in messages.Items)
    {
        Console.Write($"{item.Message}, ");
    }

    Console.WriteLine();
}