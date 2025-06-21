# Pulsar Swift Client Porting Plan

This document outlines the plan to port the .NET C# Pulsar client to Swift, focusing on achieving feature parity and adopting idiomatic Swift conventions.

## 1. Achieve Feature Parity

The initial focus should be on implementing the missing features from the C# client.

### 1.1. Complete Core Components

Several core types and options classes are missing in the Swift version. These are essential for configuring and interacting with producers, consumers, and readers.

**Action:** Create Swift equivalents for the following C# files.

| C# File (for reference) | Swift Target | Priority | Notes |
|---|---|---|---|
| `pulsar-dotpulsar/src/DotPulsar/ConsumerOptions.cs` | `Sources/PulsarClient/ConsumerOptions.swift` | High | Essential for consumer configuration. |
| `pulsar-dotpulsar/src/DotPulsar/ProducerOptions.cs` | `Sources/PulsarClient/ProducerOptions.swift` | High | Essential for producer configuration. |
| `pulsar-dotpulsar/src/DotPulsar/ReaderOptions.cs` | `Sources/PulsarClient/ReaderOptions.swift` | High | Essential for reader configuration. |
| `pulsar-dotpulsar/src/DotPulsar/MessageId.cs` | `Sources/PulsarClient/MessageId.swift` | High | The existing `Message.swift` may need to be refactored to include a proper `MessageId` type. |
| `pulsar-dotpulsar/src/DotPulsar/MessageMetadata.cs` | `Sources/PulsarClient/MessageMetadata.swift` | Medium | For handling message metadata. |
| `pulsar-dotpulsar/src/DotPulsar/Schema.cs` | `Sources/PulsarClient/Schema.swift` | High | The foundation for message schemas. |
| `pulsar-dotpulsar/src/DotPulsar/StateChanged.cs` | `Sources/PulsarClient/State.swift` | Medium | Create generic state change tracking for consumers, producers, and readers. |

### 1.2. Implement Abstractions (Protocols)

The C# client defines a rich set of interfaces (`IProducer`, `IConsumer`, etc.) in its `Abstractions` directory. The Swift client should have corresponding protocols to enable a flexible and testable architecture.

**Action:** Review the C# `Abstractions` directory and ensure that for every interface, a corresponding Swift protocol exists in `Sources/PulsarClient/Abstractions`.

*   **Reference:** `pulsar-dotpulsar/src/DotPulsar/Abstractions/`
*   **Target:** `Sources/PulsarClient/Abstractions/`

### 1.3. Port Schemas

The C# client supports various schemas for message serialization (Avro, JSON, String, etc.). The Swift client needs these implementations.

**Action:** Port the schema implementations from the C# `Schemas` directory to `Sources/PulsarClient/Schemas`.

*   **Reference:** `pulsar-dotpulsar/src/DotPulsar/Schemas/`
*   **Target:** `Sources/PulsarClient/Schemas/`

### 1.4. Port Extensions

The C# client uses extension methods to provide a convenient API. These should be replicated in Swift using extensions.

**Action:** Port the extension methods from the C# `Extensions` directory to `Sources/PulsarClient/Extensions`.

*   **Reference:** `pulsar-dotpulsar/src/DotPulsar/Extensions/`
*   **Target:** `Sources/PulsarClient/Extensions/`

## 2. Adopt Swift Idioms

To make the client feel natural to Swift developers, we should adopt modern, idiomatic Swift practices.

### 2.1. Embrace Swift Concurrency (`async/await`)

The C# client is heavily based on `async/await`. The Swift client should fully leverage Swift's modern concurrency model.

**Action:** Refactor completion-handler-based asynchronous operations to use `async/await`. This will significantly simplify the API and its usage.

*   **Areas to focus on:**
    *   `PulsarClient+Implementation.swift`
    *   Producer, Consumer, and Reader implementations.

### 2.2. Use Value Types (`struct`) where appropriate

Swift's value types (`struct` and `enum`) can improve performance and prevent unintended side effects.

**Action:** Review the models and configuration objects (like `ProducerOptions`) and convert them from classes to structs where appropriate. Data-carrying types that do not need reference semantics are good candidates.

### 2.3. Enhance Error Handling

We have already improved `PulsarClientError.swift` to be more granular.

**Action:** Ensure that the internal implementation maps the Protobuf error codes to the new, specific `PulsarClientError` cases. This will provide more precise error information to the user.

*   **Reference:** `pulsar-dotpulsar/src/DotPulsar/Internal/PulsarApi/PulsarApi.cs` (search for `ServerError`)
*   **Target:** `Sources/PulsarClient/Internal/ChannelManager+Handlers.swift`

## 3. Project Structure and Dependencies

### 3.1. Swift Package Manager

Continue to use Swift Package Manager for managing dependencies. Ensure that the `Package.swift` file is kept up-to-date as new files and dependencies are added.

### 3.2. Code Organization

The current directory structure is a good starting point. As the project grows, ensure that files are placed in the appropriate directories to maintain a clean and organized codebase. 