# MIT 6.5840: Distributed Systems Labs

This repository contains the solutions for the labs in the MIT 6.5840 Distributed Systems course. The labs involve implementing key components of distributed systems in Go, covering the following topics:

## Lab Assignments

1. **MapReduce:** Implementation of the MapReduce framework for distributed data processing.
2. **Raft Consensus Algorithm:** A distributed consensus algorithm for managing a replicated log.
3. **KV Raft:** A key-value store built on top of the Raft consensus protocol.
4. **Sharded KV Store:** An implementation of a fault-tolerant, sharded key-value store.

## Getting Started

1. Clone this repository:
   ```bash
   git clone https://github.com/your-username/your-repo.git
   cd your-repo
2. Install dependencies 
    ```bash 
    go mod tidy
3. Navigate to the specific lab directory to run and test the code.

## Lab Details
1. MapReduce
    Implements a basic MapReduce system that processes distributed tasks across multiple workers.
2. Raft Consensus
    A simplified Raft consensus algorithm implementation that handles leader election, log replication, and fault tolerance.
3. KV Raft
    A key-value store where the Raft protocol ensures consistent data replication across multiple nodes.
4. Sharded KV Store
    An advanced key-value store with sharding, where data is distributed across multiple shards to handle scalability and fault tolerance.

## References
Course Materials: MIT 6.5840 Distributed Systems

###### Generated using LLM will be modified as solutions for labs are updated.