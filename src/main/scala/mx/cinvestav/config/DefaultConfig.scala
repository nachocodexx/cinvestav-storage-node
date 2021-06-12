package mx.cinvestav.config


case class DefaultConfig(
                          nodeId:String,
                          loadBalancer:String,
                          replicationFactor:Int,
                          poolId:String,
                          exchangeName:String,
                          storagePath:String,
                          priority: Int,
                          storageNodes:List[String],
                          rabbitmq: RabbitMQConfig
                        )
