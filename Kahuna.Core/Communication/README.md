# Communication

This folder contains internal node-to-node communication abstractions used by the core managers. The public REST and gRPC endpoints live outside `Kahuna.Core`; this layer is for forwarding operations between Kahuna nodes after a manager determines that another node owns the relevant Raft partition.

`InterNode` provides implementations for:

- gRPC-based communication between real nodes.
- memory-based communication for embedded and test scenarios.

The managers use this layer through `IInterNodeCommunication` so leader redirection does not leak transport details into lock or key-value actors.
