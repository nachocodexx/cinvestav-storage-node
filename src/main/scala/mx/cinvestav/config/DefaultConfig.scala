package mx.cinvestav.config


case class DefaultConfigV5(
                          nodeId:String,
                          loadBalancer:String,
                          replicationFactor:Int,
                          poolId:String,
                          exchangeName:String,
                          storagePath:String,
                          //                          priority: Int,
                          storageNodes:List[String],
                          heartbeatTime:Int,
                          rabbitmq: RabbitMQClusterConfig,
                          port:Int,
                          replicationStrategy:String,
                          sourceFolders:List[String]
                        )
case class DefaultConfig(
                          nodeId:String,
                          loadBalancer:String,
                          replicationFactor:Int,
                          poolId:String,
                          exchangeName:String,
                          storagePath:String,
//                          priority: Int,
                          storageNodes:List[String],
                          heartbeatTime:Int,
                          rabbitmq: RabbitMQConfig,
                          port:Int,
                          replicationStrategy:String
                        )
