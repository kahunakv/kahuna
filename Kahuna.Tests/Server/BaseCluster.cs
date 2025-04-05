
using Kahuna.Server.Configuration;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Nixie;

namespace Kahuna.Tests.Server;

public abstract class BaseCluster
{
    protected static (IRaft, IKahuna) GetNode1(InMemoryCommunication communication, string walStorage, int partitions, ILogger<IRaft> raftLogger, ILogger<IKahuna> kahunaLogger)
    {
        IWAL wal = GetWAL(walStorage, raftLogger);
        
        ActorSystem actorSystem = new(logger: raftLogger);
        
        RaftConfiguration config = new()
        {
            NodeId = "kahuna1",
            Host = "localhost",
            Port = 8001,
            InitialPartitions = partitions,
            CompactEveryOperations = 100,
            CompactNumberEntries = 50
        };
        
        RaftManager raft = new(
            actorSystem, 
            config, 
            new StaticDiscovery([new("localhost:8002"), new("localhost:8003")]),
            wal,
            communication,
            new HybridLogicalClock(),
            raftLogger
        );
        
        KahunaConfiguration configuration = new()
        {
            HttpsCertificate = "",
            HttpsCertificatePassword = "",
            LocksWorkers = 8,
            BackgroundWriterWorkers = 1,
            Storage = "sqlite",
            StoragePath = "/tmp",
            StorageRevision = Guid.NewGuid().ToString(),
            DefaultTransactionTimeout = 5000,
        };

        // ActorSystem actorSystem, IRaft raft, KahunaConfiguration configuration, ILogger<IKahuna> logger
        KahunaManager kahuna = new(actorSystem, raft, configuration, kahunaLogger);
        
        raft.OnLogRestored += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError += kahuna.OnReplicationError;

        return (raft, kahuna);
    }
    
    protected static (IRaft, IKahuna) GetNode2(InMemoryCommunication communication, string walStorage, int partitions, ILogger<IRaft> raftLogger, ILogger<IKahuna> kahunaLogger)
    {
        IWAL wal = GetWAL(walStorage, raftLogger);
        
        ActorSystem actorSystem = new(logger: raftLogger);
        
        RaftConfiguration config = new()
        {
            NodeId = "kahuna2",
            Host = "localhost",
            Port = 8002,
            InitialPartitions = partitions,
            CompactEveryOperations = 100,
            CompactNumberEntries = 50
        };
        
        RaftManager raft = new(
            actorSystem, 
            config, 
            new StaticDiscovery([new("localhost:8001"), new("localhost:8003")]),
            wal,
            communication,
            new HybridLogicalClock(),
            raftLogger
        );
        
        KahunaConfiguration configuration = new()
        {
            HttpsCertificate = "",
            HttpsCertificatePassword = "",
            LocksWorkers = 8,
            BackgroundWriterWorkers = 1,
            Storage = "sqlite",
            StoragePath = "/tmp",
            StorageRevision = Guid.NewGuid().ToString(),
            DefaultTransactionTimeout = 5000,
        };

        // ActorSystem actorSystem, IRaft raft, KahunaConfiguration configuration, ILogger<IKahuna> logger
        KahunaManager kahuna = new(actorSystem, raft, configuration, kahunaLogger);
        
        raft.OnLogRestored += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError += kahuna.OnReplicationError;

        return (raft, kahuna);
    }
    
    protected static (IRaft, IKahuna) GetNode3(InMemoryCommunication communication, string walStorage, int partitions, ILogger<IRaft> raftLogger, ILogger<IKahuna> kahunaLogger)
    {
        IWAL wal = GetWAL(walStorage, raftLogger);
        
        ActorSystem actorSystem = new(logger: raftLogger);
        
        RaftConfiguration config = new()
        {
            NodeId = "kahuna3",
            Host = "localhost",
            Port = 8003,
            InitialPartitions = partitions,
            CompactEveryOperations = 100,
            CompactNumberEntries = 50
        };
        
        RaftManager raft = new(
            actorSystem, 
            config, 
            new StaticDiscovery([new("localhost:8001"), new("localhost:8002")]),
            wal,
            communication,
            new HybridLogicalClock(),
            raftLogger
        );
        
        KahunaConfiguration configuration = new()
        {
            HttpsCertificate = "",
            HttpsCertificatePassword = "",
            LocksWorkers = 8,
            BackgroundWriterWorkers = 1,
            Storage = "sqlite",
            StoragePath = "/tmp",
            StorageRevision = Guid.NewGuid().ToString(),
            DefaultTransactionTimeout = 5000,
        };

        // ActorSystem actorSystem, IRaft raft, KahunaConfiguration configuration, ILogger<IKahuna> logger
        KahunaManager kahuna = new(actorSystem, raft, configuration, kahunaLogger);
        
        raft.OnLogRestored += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError += kahuna.OnReplicationError;

        return (raft, kahuna);
    }
    
    protected static async Task WaitForClusterToAssemble(InMemoryCommunication communication, int partitions, IRaft raft1, IRaft raft2, IRaft raft3)
    {
        communication.SetNodes(new()
        {
            { "localhost:8001", raft1 }, 
            { "localhost:8002", raft2 },
            { "localhost:8003", raft3 }
        });
        
        await Task.WhenAll([raft1.JoinCluster(), raft2.JoinCluster(), raft3.JoinCluster()]);

        for (int i = 1; i <= partitions; i++)
        {
            while (true)
            {
                if (await raft1.AmILeader(i, cancellationToken: TestContext.Current.CancellationToken) ||
                    await raft2.AmILeader(i, cancellationToken: TestContext.Current.CancellationToken) ||
                    await raft3.AmILeader(i, cancellationToken: TestContext.Current.CancellationToken))
                    break;

                await Task.Delay(100, cancellationToken: TestContext.Current.CancellationToken);
            }
        }
    }

    private static IWAL GetWAL(string walStorage, ILogger<IRaft> logger)
    {
        return walStorage switch
        {
            "memory" => new InMemoryWAL(logger),
            "sqlite" => new SqliteWAL("/tmp", Guid.NewGuid().ToString(), logger),
            "rocksdb" => new RocksDbWAL("/tmp", Guid.NewGuid().ToString(), logger),
            _ => throw new ArgumentException($"Unknown wal: {walStorage}")
        };
    }
    
    private static async Task<IRaft?> GetLeader(int partitionId, IRaft[] nodes)
    {
        foreach (IRaft node in nodes)
        {
            if (await node.AmILeader(partitionId, CancellationToken.None))
                return node;
        }

        return null;
    }
    
    private static async Task<List<IRaft>> GetFollowers(IRaft[] nodes)
    {
        List<IRaft> followers = [];
        
        foreach (IRaft node in nodes)
        {
            if (!await node.AmILeader(1, CancellationToken.None))
                followers.Add(node);
        }

        return followers;
    }
}