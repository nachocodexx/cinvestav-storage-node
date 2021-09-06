package mx.cinvestav

import cats.data.EitherT
import cats.effect.{IO, Ref}
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model.AMQPConnection
import fs2.concurrent.SignallingRef
import mx.cinvestav.commons.balancer
import mx.cinvestav.commons.status.Status
import mx.cinvestav.commons.fileX.FileMetadata
import mx.cinvestav.config.DefaultConfigV5
import mx.cinvestav.utils.v2.{PublisherV2, RabbitMQContext}
import org.typelevel.log4cats.Logger
import mx.cinvestav.commons

import java.io.File

object Declarations {
  def liftFF[A]: IO[A] => EitherT[IO, NodeError, A] =  commons.liftFF[A,NodeError]
  case class StorageNode(poolId:String,nodeId:String)
  //  __________________________________________________________
  trait NodeError extends Error
  case class BadArguments(message:String) extends NodeError{
    override def getMessage: String = message
  }
  case class DownloadError(message:String) extends NodeError{
    override def getMessage: String = s"DOWNLOAD_ERROR: $message"
  }
//  __________________________________________________________
case class UploadFileOutput(sink:File,isSlave:Boolean,metadata:FileMetadata)
//
//  case class RabbitContext(client:RabbitClient[IO],connection:AMQPConnection)
  case class NodeContextV5(
                            config: DefaultConfigV5,
                            logger: Logger[IO],
                            state:Ref[IO,NodeStateV5],
                            rabbitMQContext: RabbitMQContext
                          )
  case class NodeStateV5(
                        status:Status,
                        storagesNodes: List[String] = List.empty[String],
                        loadBalancer: balancer.LoadBalancer,
                        loadBalancerPublisher:PublisherV2,
                        storageNodesPubs:Map[String,PublisherV2],
                        ip:String = "127.0.0.1",
                        availableResources:Int,
                        freeStorageSpace:Long,
                        usedStorageSpace:Long,
                        replicationStrategy:String,

                      )
}
