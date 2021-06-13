package mx.cinvestav.domain
import cats.effect.IO
import fs2.concurrent.SignallingRef
import mx.cinvestav.commons.status.Status

case class NodeState(
                    status:Status,
                    replicationFactor:Int,
                    loadBalancer:String,
                    heartbeatSignal:SignallingRef[IO,Boolean]
                    )
