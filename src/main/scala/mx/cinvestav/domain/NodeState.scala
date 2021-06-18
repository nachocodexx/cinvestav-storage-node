package mx.cinvestav.domain
import cats.effect.IO
import fs2.concurrent.SignallingRef
import mx.cinvestav.commons.status.Status
import mx.cinvestav.commons.balancer

case class NodeState(
                    status:Status,
                    replicationFactor:Int,
//                    loadBalancer:String,
                    heartbeatSignal:SignallingRef[IO,Boolean],
                    storagesNodes:List[String] = List.empty[String],
                    loadBalancer:balancer.LoadBalancer
                    )
