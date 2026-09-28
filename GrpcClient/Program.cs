using Google.Protobuf.Collections;using Grpc.Net.Client;
using GrpcServices;

var masterAddress = Environment.GetEnvironmentVariable("master-node") ?? "http://localhost:5003";
using var masterChannel = GrpcChannel.ForAddress(masterAddress);
var masterClient = new MessageService.MessageServiceClient(masterChannel);

var secondary1Address = Environment.GetEnvironmentVariable("secondary1-node") ?? "http://localhost:5001";
using var secondary1Channel = GrpcChannel.ForAddress(secondary1Address);
var secondary1Client = new MessageService.MessageServiceClient(masterChannel);

var secondary2Address = Environment.GetEnvironmentVariable("secondary2-node") ?? "http://localhost:5002";
using var secondary2Channel = GrpcChannel.ForAddress(secondary2Address);
var secondary2Client = new MessageService.MessageServiceClient(masterChannel);

Console.WriteLine(" Console application started.");
Console.WriteLine(" Type 'exit' to close the program.");

while(true)
{
    Console.WriteLine("Enter a new message to the list:");
   
    var newMessage = Console.ReadLine();
    if (string.Equals(newMessage?.Trim(), "exit", StringComparison.OrdinalIgnoreCase))
    {
        Console.WriteLine("Exiting console application...");
        break;
    }
    
    var response = await masterClient.AddMessageAsync(new MessageRequest { Message = newMessage });
    Console.WriteLine($"Success! Added message: {response.Message}");
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