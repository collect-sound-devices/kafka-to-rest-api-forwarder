# kafka-to-rest-api-forwarder (KafkaToRestApiForwarder)

A forwarding microservice for sound messages; see [WinSoundScanner](https://github.com/collect-sound-devices/win-sound-scanner-go) and [LinuxSoundScanner](https://github.com/collect-sound-devices/linux-sound-scanner).

## Motivation

KafkaToRestApiForwarder's purpose is to forward the Kafka messages produced by Linux and Windows Sound Scanners to a REST API endpoint.

## Place in *collect-sound-devices* Architecture

Kafka is the request transport between the scanner services and this forwarder.

<div style="zoom: 0.5;">

```mermaid
flowchart BT

classDef dottedBox fill:transparent, fill-opacity:0.55, stroke-dasharray:10 8, stroke-width:2px;
classDef stressedBox fill:#f0f0f0,fill-opacity:0.2,stroke-width:4px;
classDef invisibleNode fill:transparent, stroke:transparent;

coreAudioApi["Core Audio<br>(Windows API) or<br>Pulse Lib<br>(Linux PulseAudio)"]

subgraph scannerService["win-sound-scanner-go or linux-sound-scanner"]
    invisible1["<br><br><br><br><br>"]
    class invisible1 invisibleNode
    winSoundScannerService["WinSoundScanner<br>(Windows Service) or<br>LinuxSoundScanner<br>(Docker Container)"]
    invisible2["<br><br><br><br><br>"]
    class invisible2 invisibleNode
end
class scannerService dottedBox

subgraph requestQueueMicroservice["<br>"]
    requestQueue[("Request Queue<br>(RabbitMQ channel)")]
    rabbitMqRestForwarder["RmqToRestApiForwarder<br>(.NET microservice)"]
end
class requestQueueMicroservice stressedBox

deviceRepositoryApi["Device Repository Server<br>(REST API)"]

winSoundScannerService --> |Access device| coreAudioApi
coreAudioApi -->|Device events| winSoundScannerService

winSoundScannerService -->|Publish request messages| requestQueue

requestQueue -->|Fetch request messages| rabbitMqRestForwarder
rabbitMqRestForwarder --> |Detect request messages| requestQueue
rabbitMqRestForwarder -->|POST/PUT requests| deviceRepositoryApi
```
</div>



## Functions

- (Background) The Windows and Linux Sound Scanners transform the sound events into HTTP request
  messages and publish them to a colocated Kafka topic.
- KafkaToRestApiForwarder runs as a Docker container on the same machine.
- It reads the messages from a local Kafka topic and POSTs/PUTs to the configured API base URL.
- It applies debouncing of frequent volume-change PUT-requests.
  * The respective time window is configurable via `Kafka:MessageDelivery:VolumeChangeEventDebouncingWindowInMilliseconds`.
- It guarantees reliable delivery with delayed retries (*Event Forwarding Pattern*, see below)
  * It retries failed API calls before committing the consumed Kafka offset.
  * A message is published to a dead-letter topic after the retry max is reached.
  * See settings: `Kafka:MessageDelivery:RetryDelayInSeconds`, `Kafka:MessageDelivery:MaxRetryAttempts`, `Kafka:Consumer:DeadLetterTopic`.

## Event Forwarding Pattern & Debouncing

KafkaToRestApiForwarder implements a message forwarding pattern that includes debouncing
for frequent volume change events and reliable delivery with retries and a dead-letter topic.

<div style="zoom: 0.5;">

```mermaid
flowchart BT

classDef invisibleNode fill:transparent,stroke:transparent;
classDef dottedBox fill:transparent,fill-opacity:0.55, stroke-dasharray:20 5,stroke-width:2px;

subgraph scannerService["win-sound-scanner-go or linux-sound-scanner"]
    invisible1["<br><br><br><br><br>"]
    class invisible1 invisibleNode
    A["WinSoundScanner<br>(Windows Service) or<br>LinuxSoundScanner<br>(Docker Container)"]
    invisible2["<br><br><br><br><br>"]
    class invisible2 invisibleNode
end
class scannerService dottedBox


subgraph forwarder["RmqToRestApiForwarder"]
    invisible3["<br><br><br><br><br>"]
    class invisible3 invisibleNode
    B["RMQ Queue"]
    C["RabbitMqConsumerService<br>(BackgroundService)"]
    D["DebounceWorker"]
    E["SendToApiAsync"]
    G["RMQ Retry Queue<br>(.retry)"]
    H["RMQ Failed Queue<br>(.failed)"]

    invisible4["<br><br><br><br><br>"]
    class invisible4 invisibleNode
end
class forwarder dottedBox


deviceRepositoryApi["Device Repository Server<br>(REST API)"]

    A -->|"Publish HTTP messages"| B
    B -->|"Consume"| C
    C -->|"Debounce (volume events)"| D
    C -->|"Direct forward<br>(other events)"| E
    D -->|"winner message"| E
    E -->|"POST / PUT attempts"| deviceRepositoryApi


    E -->|"on failure"| G
    G -->|"TTL expires → re-deliver"| B
    E -->|"max retries exceeded"| H
```

</div>


## Technologies Used

- KafkaToRestApiForwarder:
  - **.NET 10 Worker Service / Generic Host** builds the forwarding service.
  - **Confluent.Kafka** library for interacting with Kafka.
  - **NLog** logging library for .NET.
  - Distributed as a Docker container, see `docker-compose.yml`. The respective images are built via GitHub Actions CI/CD pipeline
    and regularly published to GitHub Container Registry.
- Kafka:
  - Distributed as a Docker container, see the official Apache Kafka Docker image and `docker-compose.yml`.

## Installation

1. Install Docker Desktop on the target machine
2. Download `docker-compose.yml` from the latest release assets [Release](https://github.com/collect-sound-devices/kafka-to-rest-api-forwarder/releases/latest) into a rollout folder.
3. Create a `logs` subfolder there.
4. Use `docker compose` to bring the Kafka and kafka-to-rest-api-forwarder containers up on the host machine:<br>
   Open a PowerShell prompt in the rollout folder and run:
     ```powershell
     docker compose up -d
     ```

## Developer Environment: How to Build and Run (Windows)

1. Install Visual Studio 2026 or the .NET 10 SDK
2. Restore packages and build the solution:

    ```powershell
    # Using dotnet CLI
    dotnet build KafkaToRestApiForwarder.sln -c Release
    ```

3. (Optional) Publish a self-contained single-file for Windows x64:

    ```powershell
    # Publish with the included publish profile
    dotnet publish "Projects/KafkaToRestApiForwarder/KafkaToRestApiForwarder.csproj" -c Release -p:PublishProfile=WinX64
    ```

4. Podman vs Docker.<br>
See: [PODMAN-vs-DOCKER.md](https://github.com/collect-sound-devices/kafka-to-rest-api-forwarder/blob/HEAD/PODMAN-vs-DOCKER.md)

## Changelog
- 2026-05-25: Initialized as alternative to RabbitMQ-to-REST-API-Farwarder

## License

This project is licensed under the terms of the [MIT License](LICENSE).

## Contact

Eduard Danziger

Email: [edanziger@gmx.de](mailto:edanziger@gmx.de)
