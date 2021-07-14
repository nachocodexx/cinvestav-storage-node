package mx.cinvestav.domain
import cats.effect.IO
import fs2.concurrent.SignallingRef
import mx.cinvestav.commons.status.Status
import mx.cinvestav.commons.balancer
import mx.cinvestav.commons.storage.{Replica,FileMetadata}


case class NodeState(
                      status:Status,
                      replicationFactor:Int,
                      heartbeatSignal:SignallingRef[IO,Boolean],
                      isBeating:Boolean=false,
                      storagesNodes: List[String] = List.empty[String],
                      ipAddresses: Map[String,String] = Map.empty[String,String],
                      loadBalancer: balancer.LoadBalancer,
                      metadata:Map[String,FileMetadata] = Map.empty[String,FileMetadata],
                      ip:String = "127.0.0.1",
                      availableResources:Int,
                      //                    New
                      freeStorageSpace:Long,
                      usedStorageSpace:Long,
                      replicationStrategy:String,
                      //
                      chordRoutingKey:String,
                      activeReplicationCompletion:Map[String,Int]=Map.empty[String,Int]
            )
