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
Console.WriteLine("Write Concern test");

var task1 = await masterClient.AddMessageAsync(new MessageRequest { Message = "msg1", WriteConcern = 1 }).ResponseAsync;
await RunGetRequests();
var task2 = await masterClient.AddMessageAsync(new MessageRequest { Message = "msg2", WriteConcern = 2 }).ResponseAsync;
await RunGetRequests();
var task3 =  masterClient.AddMessageAsync(new MessageRequest { Message = "msg3", WriteConcern = 3 }).ResponseAsync;
await RunGetRequests();
var task4 = await masterClient.AddMessageAsync(new MessageRequest { Message = "msg4", WriteConcern = 1 }).ResponseAsync;
await RunGetRequests();
await RerunGetRequests(8);
Console.WriteLine();

//Testing Reordering
Console.WriteLine("ReorderingTest");

for (int i = 10; i <= 14; i++)
{
    var call = await masterClient
        .AddMessageAsync(new MessageRequest { Message = $"msg{i}", WriteConcern = 1 })
        .ResponseAsync;
}
await RunGetRequests();
await RerunGetRequests(6);

async Task RerunGetRequests(int seconds)
{
    while (seconds > 0)
    {
        Console.Write($"\rCall another Get requests to check the final state in: {seconds} seconds   ");

        Thread.Sleep(1000);
        seconds--;
    }

    Console.WriteLine();
    await RunGetRequests();
}

async Task RunGetRequests()
{
    Console.Write("Messages from master: ");
    var updatedMasterItems = await masterClient.GetMessagesAsync(new Empty());
    WriteMessages(updatedMasterItems);
    Console.Write("Messages from secondary1: ");
    var updatedSecondary1Items = await secondary1Client.GetMessagesAsync(new Empty());
    WriteMessages(updatedSecondary1Items);
    Console.Write("Messages from secondary2: ");
    var updatedSecondary2Items = await secondary2Client.GetMessagesAsync(new Empty());
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