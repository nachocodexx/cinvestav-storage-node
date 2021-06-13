# Storage node
The storage node is the basic unit of a storage system. It performs allocation and location operations as well load 
balancing and allow us to update the node state in dynamically style.

## Configuration
- **NODE_ID**: The unique identifier of the node in a storage pool.
- **LOAD_BALANCER**: Load balancing mechanisms, for example: Round Robin(RB), Pseudo Random(RND) and Two-choices(2C)
- **REPLICATION_FACTOR**: The number of copies that must exists of a file in a given storage pool.
- **POOL_ID**: The unique identifier of the storage pool. 
- **STORAGE_PATH**: Location of the written files.
- **HEARTBEAT_TIME**: Time interval of the heartbeats.

