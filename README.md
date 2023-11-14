# Synchronizer

The Synchronizer serves as a data synchronization engine between in-cluster and backend resources, designed to facilitate bi-directional data synchronization between the two. It operates as an efficient event-driven data pipeline, actively monitoring a predefined list of resources for any changes. When changes occur, it efficiently propagates these updates to the relevant endpoints.

```mermaid
flowchart LR

    subgraph Backend
    synchronizerserver["Synchronizer (server)"]
    pulsar[Pulsar]
    eventingester[Event Ingester]
    database[(Database)]
    synchronizerserver --- pulsar
    pulsar --- eventingester
    eventingester --- database
    end
    subgraph Cluster 1
    etcdcluster1[ETCD]
    synchronizercluster1["Synchronizer (client)"]
    etcdcluster1---synchronizercluster1
    end
    subgraph Cluster 2
    etcdcluster2[ETCD]
    synchronizercluster2["Synchronizer (client)"]
    etcdcluster2---synchronizercluster2
    end

    synchronizercluster2 --- synchronizerserver
    synchronizercluster1 --- synchronizerserver
```

## Running the synchronizer locally

1. Run pulsar:

    ```sh
    ./scripts/pulsar.sh
    ```

2. Start synchronizer server:

    ```sh
    CONFIG=./configuration/server go run cmd/server/main.go
    ```

3. Start synchronizer client:

    ```sh
    CONFIG=./configuration/client go run cmd/client/main.go
    ```
