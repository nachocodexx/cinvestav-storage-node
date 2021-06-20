package mx.cinvestav.domain
import cats.effect.IO
import fs2.concurrent.SignallingRef
import mx.cinvestav.commons.status.Status
import mx.cinvestav.commons.balancer

case class Replica(nodeId:String, primary:Boolean, version:Int,timestamp:Long)
case class FileMetadata(
                         originalName:String,
                         originalExtension:String,
                         size:Long,
                         compressionAlgorithm:String,
                         replicas:List[Replica]
                       )
case class NodeState(
                  status:Status,
                  replicationFactor:Int,
                  heartbeatSignal:SignallingRef[IO,Boolean],
                  isBeating:Boolean=false,
                  storagesNodes:List[String] = List.empty[String],
                  loadBalancer:balancer.LoadBalancer,
                  metadata:Map[String,FileMetadata] = Map.empty[String,FileMetadata]
//                  metadata:List[FileMetadata] = List.empty[FileMetadata]
            )
