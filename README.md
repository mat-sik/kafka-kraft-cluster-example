# Kafka Kraft Cluster Example

This project is designed for learning how to deploy a Kafka cluster with KRaft (Kafka Raft) and implement simple Java
applications using the low-level Kafka client library for producing and consuming messages.

## Features

- **Producer Application**: A straightforward implementation of a Kafka producer written in Java.
- **Consumer Application**: An advanced implementation supporting concurrent message processing and robust handling of
  offsets and partition reassignment.
- **Complete Docker Configuration**: Includes a `docker-compose.yaml` file to set up a fully functional Kafka cluster
  with KRaft.

---

## Consumer Implementation

The consumer application is designed with a focus on high performance and resilience, providing robust handling of
concurrent processing, offset management, and partition reassignments.

### 1. **Concurrent Processing**

- Each consumer instance (representing a single member of the consumer group) can spawn multiple processing threads (`N`
  threads).
- These threads process records concurrently, significantly improving throughput.
- Mapping of partitions to threads is flexible:
    - **`M` partitions → `N` processing threads**: Enables dynamic distribution of workload for scalability.

### 2. **Synchronous Acknowledgments**

- Offsets are only committed after all records up to the offset have been fully processed, ensuring **at-least-once
  delivery guarantees**.
- Concurrent processing introduces complexity, so special care is taken to handle offsets correctly:
    1. **Batch Placement**:
        - The consuming thread polls batches of records and places them into a blocking queue.
        - Processing threads pick up batches from this queue.
    2. **Batch Processing**:
        - When a processing thread completes its batch, it registers the batch's starting and ending offsets.
        - The thread checks whether all records up to the ending offset have been processed:
            - **If all records are processed**: It notifies the consumer thread to commit the offset.
            - **If not**: It assumes another processing thread will detect when the offset is ready for commit.
    3. **Duplication Handling**:
        - Message duplication can occur only if a failure happens after some records are processed but before their
          offsets are committed. This ensures a balance between reliability and performance.

### 3. **Partition Reassignment Handling**

- When a partition revocation event occurs (e.g., during rebalancing), the following steps ensure data consistency:
    1. **Notify Processing Threads**:
        - Threads stop processing new records and complete their current task (processing the current record).
    2. **Save Offsets**:
        - Processed offsets are stored in a dedicated data structure to maintain state during rebalancing.
    3. **Wait for Completion**:
        - The event listener waits for all processing threads to finish (worst-case scenario: the time required to
          process a single record).
    4. **Commit Offsets**:
        - Once all threads complete, offsets are committed, ensuring no records are lost or processed twice.

This design prioritizes consistency, reliability, and efficient use of system resources while minimizing potential
message duplication and maintaining smooth operation during partition reassignments.


---

## Docker Compose Details

The `docker-compose.yaml` file is configured to support a Kafka cluster running in KRaft (Kafka Raft) mode. This setup
includes Kafka controller and broker nodes, along with MongoDB services for consumer data storage. Below are the key
configurations and concepts used in this setup.

---

## **Docker Compose Services Overview**

### **Controller and Broker Nodes**

The Kafka cluster consists of three **controller nodes** and three **broker nodes**:

- **Controller Nodes**:
    - Manage metadata and partition assignments in KRaft mode.
    - Communicate using the `CONTROLLER` listener type.
    - Defined as `controller-1`, `controller-2`, `controller-3` with:
        - `KAFKA_PROCESS_ROLES: controller`
        - `KAFKA_CONTROLLER_QUORUM_VOTERS`: Lists all controllers in `node_id@hostname:port` format (e.g.,
          `1@controller-1:9093`).

- **Broker Nodes**:
    - Handle topic storage, message processing, and external client communication.
    - Communicate internally using the `BROKER` listener type and externally via the `CLIENT` listener type.
    - Defined as `broker-1`, `broker-2`, `broker-3` with:
        - `KAFKA_PROCESS_ROLES: broker`
        - `KAFKA_ADVERTISED_LISTENERS`: Specifies the connection details (internal and external).

### **Consumer Services**

- **MongoDB for Consumer Data** (`consumer-mongo`):
    - Stores data for consumer applications.
    - Configured with an admin username and password.

- **Mongo Express for MongoDB Management** (`consumer-mongo-express`):
    - A web-based MongoDB management tool for interacting with `consumer-mongo`.
    - Accessible through the browser on port `8081`.

---

## **Kafka Listeners**

### **Listeners**

- **Definition**: Listeners define how Kafka nodes communicate internally and externally. They consist of a name (e.g.,
  `BROKER`, `CONTROLLER`, `CLIENT`) and a port.
- **Customizability**: The listener names (`BROKER`, `CONTROLLER`, `CLIENT`) are arbitrary and can be customized to any
  meaningful identifier relevant to your setup.
    - Example: You could use `INTERNAL`, `EXTERNAL`, `MANAGER`, etc., instead of `BROKER`, `CLIENT`, `CONTROLLER`.
- **Examples**:
    - **BROKER**: Internal communication between Kafka brokers.
    - **CONTROLLER**: Communication between Kafka controllers.
    - **CLIENT**: External communication with producers and consumers.
- **Configuration**:
    - Example: `KAFKA_LISTENERS: 'BROKER://:19092,CLIENT://:9092'`
        - `BROKER` on port `19092` for inter-broker communication.
        - `CLIENT` on port `9092` for external client communication.
    - Controller Example: `controller-1`: `KAFKA_LISTENERS: CONTROLLER://:9093`.

### **Advertised Listeners**

- **Definition**: The addresses Kafka nodes advertise to other nodes and clients to ensure proper communication.
- **Purpose**: Inform nodes and clients about the connection points they can use to communicate.
- **Customizability**: Listener names used in `KAFKA_ADVERTISED_LISTENERS` must match the names defined in
  `KAFKA_LISTENERS`, even if they are custom names.
- **Configuration**:
    - Example: `KAFKA_ADVERTISED_LISTENERS: 'BROKER://broker-1:19092,CLIENT://localhost:29092'`
        - Advertises:
            - **BROKER** listener at `broker-1:19092` for internal communication.
            - **CLIENT** listener at `localhost:29092` for external clients.

---

## **Security Protocol Map**

- **Description**: Maps each listener name to a specific security protocol, defining how data is transmitted (e.g.,
  `PLAINTEXT`, `SSL`).
- **Purpose**: Essential for the communication topology within Kafka.
- **Customizability**: Every listener name (custom or default) must be defined here.
- **Example**:
    - `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,BROKER:PLAINTEXT,CLIENT:PLAINTEXT`

---

## **Key Kafka Environment Variables**

### `KAFKA_NODE_ID`

- **Description**: Unique identifier for each Kafka node (controller or broker).
- **Usage**: Differentiates nodes within the cluster.
    - Example:
        - `controller-1`: `KAFKA_NODE_ID: 1`
        - `broker-1`: `KAFKA_NODE_ID: 4`

### `KAFKA_PROCESS_ROLES`

- **Description**: Specifies the role of the Kafka node.
    - `controller`: Manages metadata and leader election.
    - `broker`: Handles storage and client communication.

### `KAFKA_INTER_BROKER_LISTENER_NAME`

- **Description**: Specifies the listener (e.g., `BROKER`) used for inter-broker communication.

### `KAFKA_CONTROLLER_LISTENER_NAMES`

- **Description**: Specifies the listener (e.g., `CONTROLLER`) for communication between controllers.

### `KAFKA_CONTROLLER_QUORUM_VOTERS`

- **Description**: Defines the quorum for controllers, listing each controller in the format `node_id@hostname:port`.
    - Example: `1@controller-1:9093,2@controller-2:9093,3@controller-3:9093`

### `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS`

- **Description**: Delay before starting rebalance operations for a consumer group (in milliseconds).
    - Example: `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0` (immediate rebalance).

---

### **Volumes**

- Volumes are used to persist Kafka and MongoDB data across container restarts. Each node (controller and broker) has
  its own data, secrets, and configuration volumes.
- MongoDB also uses dedicated volumes for database and configuration data.

---

### **Network Configuration**

- All services are connected through a common `net` network, allowing them to communicate internally within the Docker
  environment.

This project provides a hands-on opportunity to understand Kafka's architecture and implement robust client applications
in Java.
