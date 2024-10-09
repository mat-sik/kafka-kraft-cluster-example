# kafka-kraft-cluster-example

Simple project used for learning the process of deploying kafka cluster and creating
simple publisher and  consumer.

## docker-compose.yaml detials

In Kafka, listeners are just channels names that are used to communicate with
kafka nodes. The channels are used for inner broker communication - BROKER, 
kraft controller node communication - CONTROLLER and client app communication - CLIENT.

These three names are custom and can be arbitrary.

To define these channels, they should be defined in security_protocol_map.

Advertised listeners are listeners name that will be shared between nodes in the cluster.
