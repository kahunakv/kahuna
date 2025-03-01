# Kahuna

<img src="https://github.com/user-attachments/assets/a49cf165-4b65-4d87-821f-67dfc3b153d3" width="200" />

**Kahuna** is an open-source solution designed to provide robust distributed
locking for modern distributed systems. It addresses the critical challenge
of synchronizing access to shared resources across multiple nodes
or processes, ensuring consistency and preventing race conditions.
By leveraging a partitioned locking mechanism coordinated via a Raft Group,
Kahuna combines scalability, reliability and simplicity, making it an ideal choice
for applications that require coordinated access to databases, files
or other shared services.

> _Kahuna_ is a Hawaiian word that refers to an expert in any field. Historically,
it has been used to refer to doctors, surgeons and dentists,
as well as priests, ministers, and sorcerers.

---

## Table of Contents

- [Overview](#overview)
- [What Is a Distributed Lock?](#what-is-a-distributed-lock)
- [Key Features](#key-features)
- [API](#api)
- [Leases](#leases)
- [Consistency Levels](#consistency-levels) 
- [Server-Installation](#server-installation) 
- [Client-Installation](#client-installation)
- [Usage & Examples](#usage--examples)
- [Client SDK for .NET](#client-sdk-for-net)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

In modern distributed systems, it is often necessary to synchronize access to shared resources across multiple nodes or processes. Kahuna provides a robust solution for this problem by implementing distributed locks that ensure resource consistency and prevent race conditions.

By partitioning locks among nodes controlled by a Raft Group, Kahuna offers:

- **Scalability:** Multiple nodes in the cluster can handle lock requests, enabling horizontal scaling as your application grows.
- **Reliability:** Raft consensus ensures that partition data remains consistent even in the face of network failures.
- **Simplicity:** A straightforward API based on leases makes it easy to integrate distributed locking into your applications.

---

## What Is a Distributed Lock?

A distributed lock is a mechanism that ensures that a specific resource is accessed by only one node or process at a time in a distributed environment. This is crucial when:

- **Preventing race conditions:** Ensuring that multiple processes do not modify shared resources simultaneously.
- **Coordinating tasks:** Managing access to shared databases, files, or services across different nodes.
- **Maintaining data consistency:** Guaranteeing that concurrent operations do not result in inconsistent states.

---

## API

Kahuna exposes a simple API for acquiring and releasing locks. The main functions are:

### Lock

```csharp
(bool Locked, bool Errored) TryLock(string resource, string lockId, int expiresMs);
```

- **resource:** The identifier for the resource you want to lock.
- **lockId:** A unique identifier for the lock, usually associated with the client or process requesting the lock.
- **expiresMs:** The expiration time for the lock in milliseconds.

**Returns:**
- **Locked:** `true` if the lock was successfully acquired.
- **Errored:** `true` if an error occurred during the locking process.

### Unlock

```csharp
(bool Locked, bool Errored) Unlock(string resource, string lockId);
```

- **resource:** The identifier for the resource to unlock.
- **lockId:** The unique identifier for the lock previously used to acquire the lock.

**Returns:**
- **Locked:** `false` if the resource was successfully unlocked.
- **Errored:** `true` if an error occurred during the unlock process.

### Extend

```csharp
(bool Extended, bool Errored) Extend(string resource, string lockId, int expiresMs);
```

- **resource:** The identifier for the resource you want to extend.
- **lockId:** A unique identifier for the lock, usually associated with the client or process requesting the lock. It must be the current owner of the lock.
- **expiresMs:** The expiration time for the lock in milliseconds.

**Returns:**
- **Extended:** `true` if the lock was successfully extended.
- **Errored:** `true` if an error occurred during the locking process.

---

## Leases

Distributed locks in Kahuna are based on the paper [*"Leases: An Efficient
Fault-Tolerant Mechanism for Distributed File Cache Consistency"*](https://web.stanford.edu/class/cs240/readings/leases.pdf) by Michael N. Nelson, Brent B. Welch, and John K. Ousterhout.
It introduced the concept of **leases** as a way to manage distributed locks efficiently.
Leases act as time-bound locks that expire after a specified duration,
providing a balance between strong consistency and fault tolerance.

- **Automatic Lock Expiration**: Leases expire after a predefined time,
eliminating the need for manual lock release. This is particularly useful if a client holding a lock crashes or becomes unreachable, as the system can reclaim the resource once the lease expires.
- **No Need for Explicit Unlock**: Despite Kahuna clients sent explicit unlocks, clients
don't need to explicitly release them, which reduces the complexity of
handling failures and network partitions.
- **Reduced Lock Contention**: Since leases are time-bound, even if a client misbehaves or gets disconnected, other clients will eventually be able to acquire the lock after the lease expires.
- **Graceful Degradation**: In the event of partial failures (e.g., network partitions), the system can still make progress once the lease times out.

Do leases provide mutual exclusion? No, leases by themselves do not provide mutual exclusion.

While Kahuna leases help in expiring keys and releasing locks if a client fails, they don’t inherently protect against scenarios where:

- A client pauses (e.g., due to a long GC pause or network partition) and later resumes, believing it still holds the lock, even though the lease has expired.
- This could lead to split-brain where two clients believe they own the same lock.

### Fencing Tokens

A fencing token is a monotonically increasing number (e.g., version number) issued every time a lock is acquired.
It acts as a logical timestamp to resolve stale client operations.

How Leases + Fencing Tokens can provide Strong Mutual Exclusion:

#### Lock Acquisition:

A client tries to acquire a lock by creating a key in Kahuna (e.g., my-lock-resource) with a lease.
Along with the key, Kahuna maintains a fencing token — typically an incrementing counter.

#### Using the Fencing Token:

When a client successfully acquires the lock, it receives the fencing token.
All downstream services that the client interacts with must validate the fencing token.
These services should reject any operation with a stale fencing token (i.e., a token lower than the highest one they've seen).

#### Handling Client Failures:

If a client pauses or crashes and its lease expires, Kahuna deletes the lock key.
Another client can now acquire the lock with a new lease and gets a higher fencing token.
Even if the first client resumes and tries to perform actions, downstream systems will reject its operations because its fencing token is outdated.

Example Flow:

- Client A acquires the lock with fencing token #5.
- Client A writes to a resource, passing #5.
- Client A experiences a network partition or pause.
- Kahuna lease expires, and Client B acquires the lock with fencing token #6.
- Client B writes to the same resource, passing #6.
- Client A comes back online and tries to write again with fencing token #5, but downstream systems reject it because they've already processed token #6.

---

## Consistency Levels

Kahuna provides different consistency levels to meet the requirements of various applications:

| Consistency Level      | Replication Mechanism                                  | Leader Role                                         | Lock State Storage      | Use Case                                                                 | Failure Impact                                                   |
|------------------------|--------------------------------------------------------|-----------------------------------------------------|-------------------------|-------------------------------------------------------------------------|------------------------------------------------------------------|
| Strong Consistency    | Raft consensus replicates across all nodes             | Raft consensus ensures consistency                 | Persisted across nodes  | Locks with long-duration TTLs, where failures cause serious issues      | Critical – ensures state consistency across all nodes            |
| Ephemeral Consistency | Lock state kept in memory, not replicated              | Leaders manage lock state, Raft handles re-election | Only in leader memory  | Locks with short-duration TTLs (<10 sec), where failure recovery is quick | Minimal – recovery from persistence adds little value            |

## Server Installation

### Standalone server

You can build and run the Kahuna server using the following steps (it requires .NET 9.0 installed):

```bash
git clone https://github.com/andresgutierrez/kahuna
cd kahuna
export ASPNETCORE_URLS='http://*:2070'
dotnet run --project Kahuna.Server
```

### Local Docker container

Alternatively, you can run the Kahuna server in a local Docker container:

```bash
git clone https://github.com/andresgutierrez/kahuna
cd kahuna
docker build -f Dockerfile -t kahuna .
docker run -e ASPNETCORE_URLS='http://*:2070' -p 2070:2070 kahuna
```

### Local Docker Compose Cluster

To run a local cluster of Kahuna servers using Docker Compose:

```bash
git clone https://github.com/andresgutierrez/kahuna
cd kahuna
docker build -f Dockerfile -t kahuna .
docker compose up
```

---

## Client Installation

Kahuna Client for .NET is available as a NuGet package. You can install it via the .NET CLI:

```bash
dotnet add package Kahuna.Client --version 0.0.3
```

Or via the NuGet Package Manager:

```powershell
Install-Package Kahuna.Client -Version 0.0.3
```

---

## Usage & Examples

### Single attempt to acquire a lock

Below is a basic example to demonstrate how to use Kahuna in a C# project:

```csharp
using Kahuna.Client;

// Create a Kahuna client (it can be a global instance)
var client = new KahunaClient("http://localhost:2070");

// ...

public async Task UpdateBalance(KahunaClient client, string userId)
{
    // try to lock on a resource using a keyName composed of a prefix and the user's id,
    // if acquired then automatically release the lock after 5 seconds (if not extended),
    // it will give up immediately if the lock is not available,
    // if the lock is acquired it will prevent the same user from changing the same data concurrently

    await using KahunaLock myLock = await client.GetOrCreateLock("balance-" + userId, TimeSpan.FromSeconds(5));

    if (myLock.IsAcquired)
    {
        Console.WriteLine("Lock acquired!");

        // implement exclusive logic here
    }
    else
    {
        Console.WriteLine("Someone else has the lock!");
    }

    // myLock is automatically released after leaving the method
}
```

### Multiple attempts to acquire a lock

The following example shows how to make multiple attempts to
acquire a lock (lease) for 10 seconds, retrying every 100 ms:

```csharp
using Kahuna.Client;

public async Task UpdateBalance(KahunaClient client, string userId)
{
    // try to lock on a resource using a keyName composed of a prefix and the user's id,
    // if acquired then automatically release the lock after 5 seconds (if not extended),
    // if not acquired retry to acquire the lock every 100 milliseconds for 10 seconds,
    // it will give up after 10 seconds if the lock is not available,
    // if the lock is acquired it will prevent the same user from changing the same data concurrently

    await using KahunaLock myLock = await client.GetOrCreateLock(
        "balance-" + userId,
        expiry: TimeSpan.FromSeconds(5),
        wait: TimeSpan.FromSeconds(10),
        retry: TimeSpan.FromMilliseconds(100)
    );

    if (myLock.IsAcquired)
    {
        Console.WriteLine("Lock acquired!");

        // implement exclusive logic here
    }
    else
    {
        Console.WriteLine("Someone else has the lock!");
    }

    // myLock is automatically released after leaving the method
}
```

### Fencing Tokens

Whenever possible, it is also important to use the fencing tokens.
Even if a client thinks it holds the lock post-lease expiration, fencing tokens prevent stale writes.
In this example, the fencing token is used to perform optimistic locking:

```csharp
using Kahuna.Client;

public async Task IncreaseBalance(KahunaClient client, string userId, long amount)
{
    // try to lock on a resource holding the lease for 5 seconds
    // and prevent stale clients from modifying data after losing their lock.

    await using KahunaLock myLock = await client.GetOrCreateLock(
        "balance-" + userId,
        expiry: TimeSpan.FromSeconds(5)
    );

    if (myLock.IsAcquired)
    {
        Console.WriteLine("Lock acquired!");

        BalanceAccount account = await db.GetBalance(userId);

        if (account.FencingToken > myLock.FencingToken)
        {
            // Write rejected: Stale fencing token

            Console.WriteLine("Someone else had the lock!");
            return;
        }

        // Write successful: New balance saved with new fencing token

        account.Balance += amount;
        account.FencingToken = myLock.FencingToken;

        await db.Save(account);
    }
    else
    {
        Console.WriteLine("Someone else has the lock!");
    }

    // myLock is automatically released after leaving the method
}
```

### Periodically extend a lock

At times, it is useful to periodically extend the lock's expiration
time while a client holds it, for example, in a leader election scenario.
As long as the leader node is alive and healthy, it can extend the
lock duration to signal that it can continue acting as the leader:

```csharp
using Kahuna.Client;

public async Task TryChooseLeader(KahunaClient client, string groupId)
{
    await using KahunaLock myLock = await client.GetOrCreateLock(
        "group-leader-" + groupId,
        expiry: TimeSpan.FromSeconds(5)
    );

    if (!myLock.IsAcquired)
    {
        Console.WriteLine("Lock not acquired!");
        return;
    }

    while (true)
    {
        bool isExtended = await myLock.TryExtend(TimeSpan.FromSeconds(5));
        if (!isExtended)
        {
            Console.WriteLine("Lock extension failed!");
            break;
        }

        // extend the lock every 5 seconds
        await Task.Delay(5000);
    }
}
```

### Retrieve information about a lock

You can also retrieve information about a lock, such as the current lock's owner 
and remaining time for the lock to expire:

```csharp
using Kahuna.Client;

public async Task TryChooseLeader(KahunaClient client, string groupId)
{
    await using KahunaLock myLock = await client.GetOrCreateLock(
        "group-leader-" + groupId, 
        expiry: TimeSpan.FromSeconds(5)
    );

    if (!myLock.IsAcquired)
    {
        Console.WriteLine("Lock not acquired!");
        
        var lockInfo = await myLock.GetInfo();
        
        Console.WriteLine($"Lock owner: {lockInfo.Owner}");
        Console.WriteLine($"Expires: {lockInfo.Expires}");       
    }                                                             
}
```

### Configure a pool of endpoints

If you want to configure a pool of Kahuna endpoints belonging to the 
same cluster so that traffic is distributed in a round-robin manner:

```csharp
using Kahuna.Client;

// Create a Kahuna client with a pool of endpoints
var client = new KahunaClient([
    "http://localhost:8081",
    "http://localhost:8082",
    "http://localhost:8083"
]);

// ...
```

### Specify consistency level

You can also specify the desired consistency level when acquiring a lock:

```csharp
using Kahuna.Client;

public async Task UpdateBalance(KahunaClient client, string userId)
{
    // acquire a lock with strong consistency, ensuring that the lock state is 
    // replicated across all nodes in the Kahuna cluster
    // in case of failure or network partition, the lock state is guaranteed to be consistent
    
    await using KahunaLock myLock = await client.GetOrCreateLock(
        "balance-" + userId, 
        TimeSpan.FromSeconds(300), // lock for 5 mins
        consistency: KahunaLockConsistency.Consistent
    );

    if (myLock.IsAcquired)
    {
        Console.WriteLine("Lock acquired with strong consistency!");

        // implement exclusive logic here
    }
    else
    {
        Console.WriteLine("Someone else has the lock!");
    }

    // myLock is automatically released after leaving the method
}
```

---

## Client SDK for .NET

Kahuna also provides a client tailored for .NET developers. This SDK simplifies the integration of distributed locking into your .NET applications by abstracting much of the underlying complexity. Documentation and samples for the client SDK can be found in the `docs/` folder or on our [GitHub repository](https://github.com/andresgutierrez/kahuna).

---

## Contributing

We welcome contributions from the community! For detailed guidelines, refer to our [CONTRIBUTING.md](CONTRIBUTING.md) file.

---

## License

Kahuna is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

Kahuna offers a robust, scalable, and reliable solution for distributed locking in modern systems, making it an invaluable tool for developers facing the challenges of distributed resource management.
