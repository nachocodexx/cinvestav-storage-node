package mx.cinvestav

import cats.data.EitherT
import cats.effect.{IO, Ref}
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model.AMQPConnection
import fs2.concurrent.SignallingRef
import mx.cinvestav.commons.balancer
import mx.cinvestav.commons.status.Status
import mx.cinvestav.commons.storage.FileMetadata
import mx.cinvestav.config.DefaultConfigV5
import mx.cinvestav.utils.v2.PublisherV2
import org.typelevel.log4cats.Logger

import java.io.File

object Declarations {
  def liftFF[A] = EitherT.liftF[IO,NodeError,A](_)
  //  __________________________________________________________
  trait NodeError extends Error
//  case class
//  __________________________________________________________
case class UploadFileOutput(sink:File,isSlave:Boolean)
//
  case class RabbitContext(client:RabbitClient[IO],connection:AMQPConnection)
  case class NodeContextV5(
                            config: DefaultConfigV5,
                            logger: Logger[IO],
                            state:Ref[IO,NodeStateV5],
                            rabbitContext: RabbitContext
                          )
  case class NodeStateV5(
                        status:Status,
                        replicationFactor:Int,
                        heartbeatSignal:SignallingRef[IO,Boolean],
                        isBeating:Boolean=false,
                        storagesNodes: List[String] = List.empty[String],
                        ipAddresses: Map[String,String] = Map.empty[String,String],
                        loadBalancer: balancer.LoadBalancer,
                        metadata:Map[String,FileMetadata] = Map.empty[String,FileMetadata],
                        storageNodesPubs:Map[String,PublisherV2],
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
}
