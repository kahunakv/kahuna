// See https://aka.ms/new-console-template for more information

using Kahuna.Client;

//Console.WriteLine("Hello, World!");

var x = new KahunaClient("http://localhost:2070");

await using KahunaLock p = await x.GetOrCreateLock("hello3", TimeSpan.FromSeconds(3));

if (p.IsAcquired)
    Console.WriteLine("adquired");