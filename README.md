# kafka-kraft-cluster-example

Project used for learning the process of deploying kafka cluster with kraft and implementing simple producer java app 
and concurrent consumer java app.

## Consumer

The consumer implementation is pretty advanced. For each consumer instance (which represents single consumer in the 
consumer group) there can be N processing threads. The processing threads are responsible for concurrent processing of 
records that are polled by the consumer from a given topic. 

The consumer makes use of synchronous acknowledgments and can handle reassignment events. The implementation should
minimize message delivery duplication, by smartly handling committed offsets.

When partition revoke event is received, the processing threads are informed of it and should stop processing and save
it processed offsets to special data structure. This in the worst case can take the time
to process a single record. Once each of the processing threads is done, the event listener can continue, it commits
the offsets and is done.

## docker-compose.yaml details

In Kafka, listeners are just channel names that are used to communicate with
kafka nodes. The channels are used for inner broker communication - BROKER, 
kraft controller node communication - CONTROLLER and client app communication - CLIENT.

This is kind of like a definition of communication topology in the cluster.

These three names are custom and can be arbitrary.

To define these channels, they should be defined in security_protocol_map.

Advertised listeners are listener names that will be shared between nodes in the cluster.
