# Gossip-rs

Gossip-rs is a simple implementation of the Raft consensus algorithm in Rust. This project is intended for educational purposes to demonstrate the core concepts of Raft, including leader election, log replication, and safety.

## Features

*   Leader election and heartbeat mechanism.
*   Log replication and entry commitment.
*   Simulated network with message dropping and delays to test robustness.
*   Basic client request handling.

## Project Structure

The project is organized into the following modules:

*   `main.rs`: The entry point of the application, responsible for setting up the network simulation and Raft nodes.
*   `node.rs`: Contains the core implementation of the Raft protocol, including the state machine for followers, candidates, and leaders.
*   `event.rs`: Defines the data structures for messages exchanged between nodes, such as `RequestVote`, `AppendEntries`, and `Vote`.
*   `timer.rs`: Provides a timer service for managing election and heartbeat timeouts.

## Getting Started

### Prerequisites

*   Rust programming language and Cargo package manager. You can install them from [rust-lang.org](https://www.rust-lang.org/).

### Building and Running

1.  Clone the repository:
    ```sh
    git clone https://github.com/peixy0/gossip-rs.git
    cd gossip-rs
    ```

2.  Build the project:
    ```sh
    cargo build
    ```

3.  Run the simulation:
    ```sh
    RUST_LOG=info cargo run
    ```
    This will start a simulation with 5 Raft nodes. You can adjust the log level by changing the `RUST_LOG` environment variable (e.g., `RUST_LOG=debug`).

## How It Works

The simulation starts by creating a specified number of Raft nodes and a central "network" channel. Each node runs in its own asynchronous task and communicates with other nodes by sending messages through the network channel.

The network simulator task introduces unreliability by randomly dropping messages and adding delays to simulate real-world network conditions.

A separate client request injector task sends requests to random nodes in the cluster. The Raft protocol ensures that these requests are forwarded to the leader and replicated across the cluster.
