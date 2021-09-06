package mx.cinvestav.config


case class LoadBalancerInfo(
                           strategy:String,
                           exchange:String,
                           routingKey:String
                           )

case class DefaultConfigV5(
                          nodeId:String,
//                          loadBalancer:String,
                          loadBalancer:LoadBalancerInfo,
                          replicationFactor:Int,
                          poolId:String,
//                          exchangeName:String,
                          storagePath:String,
                          storageNodes:List[String],
                          rabbitmq: RabbitMQClusterConfig,
                          port:Int,
                          host:String,
                          replicationStrategy:String,
//                          sourceFolders:List[String]
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
