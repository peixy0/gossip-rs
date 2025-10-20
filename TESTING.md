# Testing Guidelines

This document outlines the principles and guidelines for writing tests for this Raft implementation. The primary goal is to create a deterministic, robust, and maintainable test suite that can reliably verify the correctness of the consensus algorithm.

## Core Principles

- **Determinism:** Tests must be deterministic. The outcome of a test should be the same every time it is run, regardless of timing or environment. This means avoiding dependencies on real-time clocks or thread scheduling.
- **Clarity:** Tests should be easy to read and understand. A clear test serves as documentation for the behavior it verifies.
- **Isolation:** Tests should be isolated from one another. The failure of one test should not impact the outcome of another.

## Driving the State Machine with Internal Events

The Raft implementation is modeled as an event-driven state machine. To maintain deterministic control, tests should directly interact with this state machine by sending `Inbound` events.

- **Avoid Real Network I/O:** Do not use actual network sockets. Tests should capture `Outbound` events from the node and simulate receiving `Inbound` events from other nodes. This provides complete control over the network interactions and timing.
- **Simulate Scenarios:** Construct test scenarios by sending a controlled sequence of events to the nodes under test. For example, to test a leader election, a test would manually send `InitiateElection` to one node, then feed the resulting `RequestVote` events to other nodes, and finally send the `Vote` responses back to the candidate.

## Timer Abstraction

To eliminate dependencies on real-time, the system uses a `TimerService` trait.

- **Use the `NoopTimerService`:** All tests must use the provided `NoopTimerService`. This is a mock implementation that does not rely on `tokio::time::sleep` or any real-time delays. Since the timer is a no-op, tests that need to verify timeout-related logic (like elections) should trigger these events manually (e.g., by sending `InitiateElection`).

## Code Modifications for Testing

It is sometimes necessary to access internal components for testing, but this should be done with care to avoid breaking encapsulation.

- **Minimal Visibility Changes:** If a struct, enum, or function needs to be accessed by a test, its visibility should be changed to `pub(crate)`. **Never** make an item `pub` solely for the purpose of testing. The `#[cfg(test)]` attribute can also be used to expose helper functions or implementations only during testing.
- **No Test-Specific Events:** Do not add new variants to the `Inbound` or `Outbound` enums just to facilitate a test. The existing event vocabulary should be sufficient to drive the state machine through any valid scenario. Adding events for testing purposes pollutes the core logic and indicates a potential flaw in the test design.
