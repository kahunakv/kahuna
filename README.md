# Kahuna

Kahuna is an open-source server for managing distributed locks in a scalable and fault-tolerant manner. It harnesses the power of distributed systems by enabling the addition of multiple nodes and distributing lock management across partitions managed by a Raft Group. The API is simple yet effective, ensuring that only one process can access a resource at any given time.

It supports many servers (nodes) using Raft groups to elect leaders for different partitions, with different keys belonging to these partitions. For development purposes, Kahuna can also be used as a standalone server without horizontal scalability.

In addition, Kahuna provides a client library for .NET to connect to the server, with plans to offer clients for other languages as well.

---

## Table of Contents

- [Overview](#overview)
- [What Is a Distributed Lock?](#what-is-a-distributed-lock)
- [Key Features](#key-features)
- [API](#api)
- [Installation](#installation)
- [Usage](#usage)
- [Client SDK for .NET](#client-sdk-for-net)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

In modern distributed systems, it is often necessary to synchronize access to shared resources across multiple nodes or processes. Kahuna provides a robust solution for this problem by implementing distributed locks that ensure resource consistency and prevent race conditions.

By partitioning locks among nodes controlled by a Raft Group, Kahuna offers:

- **Scalability:** Easily add more nodes to handle increased load.
- **Reliability:** Raft consensus ensures that partition data remains consistent even in the face of network failures.
- **Simplicity:** A straightforward API makes it easy to integrate distributed locking into your applications.

---

## What Is a Distributed Lock?

A distributed lock is a mechanism that ensures that a specific resource is accessed by only one node or process at a time in a distributed environment. This is crucial when:

- **Preventing race conditions:** Ensuring that multiple processes do not modify shared resources simultaneously.
- **Coordinating tasks:** Managing access to shared databases, files, or services across different nodes.
- **Maintaining data consistency:** Guaranteeing that concurrent operations do not result in inconsistent states.

By following concepts similar to those in Redis’ Redlock, Kahuna provides a robust strategy to implement distributed locking while handling the complexities of network communication and node failures.

---

## Key Features

- **Scalability:** Seamlessly scale your system by adding more nodes.
- **Fault Tolerance:** Utilizes a Raft Group for consensus, ensuring high availability even during node failures.
- **Simplicity:** Easy-to-use API with minimal setup.
- **Cross-Platform:** Built for the .NET ecosystem, ensuring broad compatibility with C# applications.
- **Inspired by Redis’ Redlock:** Adopts proven ideas to implement secure and reliable distributed locks.

---

## API

Kahuna exposes a simple API for acquiring and releasing locks. The main functions are:

### TryLock

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

---

## Installation

Kahuna is available as a NuGet package. You can install it via the .NET CLI:

```bash
dotnet add package Kahuna.Client
```

Or via the NuGet Package Manager:

```powershell
Install-Package Kahuna.Client
```

---

## Usage

Below is a basic example to demonstrate how to use Kahuna in your C# project:

```csharp
using Kahuna.Client;

// Create a Kahuna client (it can be a global instance)
var client = new KahunaClient("http://localhost:2070");

public async Task UpdateTokens(string userId)
{
    // lock on a resource using a keyName composed of a prefix and the user's id,
    // this will give up immediately if the lock is not available,
    // if the lock is acquired it will prevent the same user from changing the same data concurrently
    await using KahunaLock myLock = await client.GetOrCreateLock("tokens-" + userId, TimeSpan.FromSeconds(5));

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

---

## Client SDK for .NET

Kahuna also provides a client SDK tailored for .NET developers. This SDK simplifies the integration of distributed locking into your .NET applications by abstracting much of the underlying complexity. Documentation and samples for the client SDK can be found in the `docs/` folder or on our [GitHub repository](https://github.com/andresgutierrez/kahuna).

---

## Contributing

We welcome contributions from the community! For detailed guidelines, refer to our [CONTRIBUTING.md](CONTRIBUTING.md) file.

---

## License

Kahuna is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

Kahuna provides a robust and scalable solution for managing distributed locks in your .NET applications. By leveraging the principles of Raft consensus and the proven strategies from Redis’ Redlock, Kahuna ensures that your critical sections remain safe and synchronized in distributed environments. Happy coding!