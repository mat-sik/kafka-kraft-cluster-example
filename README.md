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

The consumer application is designed with a focus on high performance and resilience, providing robust handling of concurrent processing, offset management, and partition reassignments.

### 1. **Concurrent Processing**
- Each consumer instance (representing a single member of the consumer group) can spawn multiple processing threads (`N` threads).
- These threads process records concurrently, significantly improving throughput.
- Mapping of partitions to threads is flexible:
    - **`M` partitions → `N` processing threads**: Enables dynamic distribution of workload for scalability.

### 2. **Synchronous Acknowledgments**
- Offsets are only committed after all records up to the offset have been fully processed, ensuring **at-least-once delivery guarantees**.
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
        - Message duplication can occur only if a failure happens after some records are processed but before their offsets are committed. This ensures a balance between reliability and performance.

### 3. **Partition Reassignment Handling**
- When a partition revocation event occurs (e.g., during rebalancing), the following steps ensure data consistency:
    1. **Notify Processing Threads**:
        - Threads stop processing new records and complete their current task (processing the current record).
    2. **Save Offsets**:
        - Processed offsets are stored in a dedicated data structure to maintain state during rebalancing.
    3. **Wait for Completion**:
        - The event listener waits for all processing threads to finish (worst-case scenario: the time required to process a single record).
    4. **Commit Offsets**:
        - Once all threads complete, offsets are committed, ensuring no records are lost or processed twice.

This design prioritizes consistency, reliability, and efficient use of system resources while minimizing potential message duplication and maintaining smooth operation during partition reassignments.


---

## Docker Compose Details

The `docker-compose.yaml` file is configured to support a Kafka cluster running in KRaft (Kafka Raft) mode. This setup includes Kafka controller and broker nodes, along with MongoDB services for consumer data storage. Below are the key configurations and concepts used in this setup.

### **Listeners**
- **Definition**: Listeners are named communication channels used for internal and external communication between Kafka nodes and clients.
- **Custom Listener Types**:
    1. **BROKER**: For internal communication between Kafka brokers.
    2. **CONTROLLER**: For communication between KRaft controller nodes.
    3. **CLIENT**: For communication with external client applications (e.g., Kafka producers, consumers).

- These listener names (BROKER, CONTROLLER, CLIENT) are customizable and can be adjusted as needed.

### **Security Protocol Map**
- This configuration defines the communication topology of the Kafka cluster by mapping listener names to specific security protocols.

### **Advertised Listeners**
- These are the listener names that are shared with other Kafka nodes and external clients.
- They ensure proper communication by informing nodes and clients about the available connection points.

---

### **Docker Compose Services Overview**

This configuration sets up three controller nodes and three broker nodes, with MongoDB services for consumer applications.

#### **Controller Nodes**
- The KRaft mode relies on multiple controller nodes to manage metadata and partition assignments.
- The three controller nodes (`controller-1`, `controller-2`, `controller-3`) are configured with the environment variable `KAFKA_PROCESS_ROLES: controller`.
- Each controller node communicates with others using the `CONTROLLER` listener type and shares quorum information through `KAFKA_CONTROLLER_QUORUM_VOTERS`.

#### **Broker Nodes**
- The Kafka broker nodes (`broker-1`, `broker-2`, `broker-3`) are configured with the `BROKER` listener for internal communication and the `CLIENT` listener for communication with external applications.
- The `KAFKA_ADVERTISED_LISTENERS` environment variable ensures brokers advertise their connection details (both internal and external).

#### **Consumer Services**
- **MongoDB for Consumer Data** (`consumer-mongo`):
    - A MongoDB service that stores data for consumer applications.
    - It is configured with an admin username and password for initial setup.

- **Mongo Express for MongoDB Management** (`consumer-mongo-express`):
    - A web-based MongoDB management tool for interacting with the `consumer-mongo` database.
    - It is accessible through the browser on port 8081 for easy database management.

---

### **Volumes**
- Volumes are used to persist Kafka and MongoDB data across container restarts. Each node (controller and broker) has its own data, secrets, and configuration volumes.
- MongoDB also uses dedicated volumes for database and configuration data.

---

### **Network Configuration**
- All services are connected through a common `net` network, allowing them to communicate internally within the Docker environment.


This project provides a hands-on opportunity to understand Kafka's architecture and implement robust client applications
in Java.
