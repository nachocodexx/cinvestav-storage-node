package mx.cinvestav
import cats.data.EitherT

import java.io.{File, FileOutputStream}
import java.net.URL
import java.nio.channels.Channels
import java.nio.channels.ReadableByteChannel
import cats.implicits._
import cats.effect.IO
import dev.profunktor.fs2rabbit.model.ExchangeType
import fs2.concurrent.SignallingRef
import mx.cinvestav.domain.Errors.{CompressionFail, DecompressionFail, DuplicatedReplica, Failure, FileNotFound}
import mx.cinvestav.commons.commands.CommandData
import mx.cinvestav.commons.payloads
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.domain.{CommandId, FileMetadata, NodeState, Payloads, Replica}
import mx.cinvestav.utils.RabbitMQUtils
import org.typelevel.log4cats.Logger
//import scodec.Attempt.Failure

import scala.util.{Success, Try}
//
import io.circe.Json
import io.circe.syntax._
import io.circe.generic.auto._
import io.circe.{Decoder, Json}
import scala.concurrent.duration._
import scala.language.postfixOps
import mx.cinvestav.utils.PublisherNode
import com.github.gekomad.scalacompress.Compressors._
import com.github.gekomad.scalacompress.CompressionStats
import com.github.gekomad.scalacompress.DecompressionStats

class Helpers()(implicit utils: RabbitMQUtils[IO],config: DefaultConfig,logger: Logger[IO]){

  def createFileMetadataE(payload: Payloads.UploadFile,file: File):EitherT[IO, Failure, FileMetadata] = {
      val fileMetadata:EitherT[IO,Failure,FileMetadata] = for {
        timestamp     <- EitherT(IO.realTime.map(x=>(x.toSeconds/1000L).asRight))
        replica       <- EitherT(
          IO.pure(Replica(config.nodeId,primary = true,0,timestamp).asRight)
        )
        fileMetadata  <- EitherT[IO,Failure,FileMetadata](
          FileMetadata(
            originalName = payload.filename,
            originalExtension = payload.extension,
            compressionAlgorithm = "lz4",
            size = file.length(),
            replicas = replica::Nil,
            compressionExt = "lz4"
          ).asRight[Failure].pure[IO]
        )
      } yield fileMetadata
      fileMetadata
  }
//
//  def createFileMetadata(previousMeta:Option[FileMetadata],payload: Payloads.UploadFile,file: File):IO[FileMetadata]
//  = previousMeta match {
//    case Some(value) =>  IO.raiseError(new Throwable("FILE ALREADY EXISTS"))
//    case None =>
//      for {
//        timestamp     <- IO.realTime.map(_.toSeconds/1000L)
//        replica       <- Replica(config.nodeId,payload.nodes.isEmpty,0,timestamp).pure[IO]
//        fileMetadata  <- FileMetadata(
//          originalName = payload.filename,
//          originalExtension = payload.extension,
//          compressionAlgorithm = "LZ4",
//          size = file.length(),
//          replicas = replica::Nil,
//          compressionExt = "lz4"
//        ).pure[IO]
//      } yield fileMetadata
//  }


  def replicate(currentState:NodeState,payload: Payloads.Replication): IO[Unit] = for {
    _              <- Logger[IO].debug(CommandId.REPLICATION+s" ${payload.id}")
    lb             <-  currentState.loadBalancer.pure[IO]
    storageNodes   <- currentState.storagesNodes.pure[IO]
    availableNodes <- storageNodes.toSet.diff(
      payload.nodes.filter(_!=config.nodeId).toSet
    ).toList.pure[IO]
    nodeId     <- lb.balance(availableNodes).pure[IO]
//    replicationPayload = payload.copy(url = currentState.ip,nodes = payload.nodes:+nodeId)
    publisher <- fromNodeIdToPublisher(nodeId,s"${config.poolId}.$nodeId.default")
    cmd       <- CommandData[Json](CommandId.REPLICATION,payload.asJson).pure[IO]
    _         <- publisher.publish(cmd.asJson.noSpaces)
    _         <- Logger[IO].debug(s"SENT_REPLICATION_CMD ${payload.id} $nodeId")

  } yield ()

//  def continueReplication(currentState:NodeState, payload: Payloads.UploadFile)(implicit config: DefaultConfig): IO[Unit] = for {
//    lb             <-  currentState.loadBalancer.pure[IO]
//    storageNodes   <- currentState.storagesNodes.pure[IO]
//    availableNodes <- storageNodes.toSet.diff(
//      payload.nodes.filter(_!=config.nodeId).toSet
//    ).toList.pure[IO]
//    _              <- Logger[IO].debug("AVAILABLE_NODES "+availableNodes.mkString(","))
//    nodeId     <- lb.balance(availableNodes).pure[IO]
//    uploadPayload    <- Payloads.UploadFile(
//      id          = payload.id,
//      fileId      = payload.fileId,
//      userId      = payload.userId,
//      url         = payload.url,
////      url         = currentState.ip,
//      replicas    = payload.replicas-1,
//      nodes       = payload.nodes:+ config.nodeId,
//      filename    = payload.filename,
////      filename    = payload.fileId,
//      extension   = payload.extension,
////      extension   = payload.compressionAlgorithm,
//    ).pure[IO]
//    publisher <- fromNodeIdToPublisher(nodeId,s"${config.poolId}.$nodeId.default")
//    cmd       <- CommandData[Json](CommandId.UPLOAD_FILE,uploadPayload.asJson).pure[IO]
//    _         <- publisher.publish(cmd.asJson.noSpaces)
//    _         <- Logger[IO].debug(s"SEND_FILE ${payload.id} $nodeId")
//  } yield ()

  def fromNodeIdToPublisher(nodeId:String,routingKey:String,metadata:Map[String, String]=Map.empty[String, String]): IO[PublisherNode] =
    for {
    publish <- utils.createPublisher(config.poolId,routingKey)
    pubInfo <- PublisherNode(nodeId,publish,metadata).pure[IO]
  }  yield pubInfo

  def _startHeart(heartbeatSignal:SignallingRef[IO,Boolean]): IO[Unit] =  for {
    _               <- heartbeatSignal.set(false)
    heartbeatQueue  <- s"${config.poolId}-heartbeat".pure[IO]
    heartbeatRk     <- s"${config.poolId}.heartbeat".pure[IO]
    _               <- utils.createQueue(heartbeatQueue,config.poolId,ExchangeType.Topic,heartbeatRk)
    //        HEARTBEAT PUBLISHER
    heartbeatPublisher <- utils.createPublisher(config.poolId,heartbeatRk)
    _                  <- utils
      .publishEvery(this.heartbeat(_,heartbeatPublisher),config.heartbeatTime milliseconds)
      .interruptWhen(heartbeatSignal)
      .compile.drain.start
  } yield ()

  def heartbeat(value:Int,publisher:String=>IO[Unit]):IO[Unit] =
    for {
      beat     <- payloads.HeartbeatPayload(value=value,config.nodeId).pure[IO]
      beatJson <- beat.asJson.pure[IO]
      command  <- CommandData[Json](CommandId.HEARTBEAT,payload  = beatJson).pure[IO]
      _        <- publisher(command.asJson.noSpaces)
      _        <- Logger[IO].trace(s"HEARTBEAT ${beat.value} ${config.nodeId}")
    } yield ()


  def transferE(filename:String, fos:FileOutputStream,rbc:ReadableByteChannel): EitherT[IO, Failure, Long] =
    EitherT(
      fos.getChannel.transferFrom(rbc,0,Long.MaxValue)
        .pure[IO]
        .map(_.asRight[Failure])
        .handleErrorWith{ t =>
          val res:Either[Failure,Long] = FileNotFound(filename).asLeft[Long]
          IO.pure(res)
        }
    )

  def newChannelE(filename:String,url: URL): Either[FileNotFound, ReadableByteChannel] = Either.fromTry(
    Try {
      Channels.newChannel(url.openStream())
    }
  )
    .flatMap(_.asRight[Failure])
    .leftFlatMap{ t=>
   Left(FileNotFound(filename))
  }
  def saveFileE(payload:Payloads.UploadFile): EitherT[IO,Failure,File] = {
    for {
      _             <- Logger.eitherTLogger[IO,Failure].debug(s"SAVE_FILE_INIT ${payload.id}")
      completeUrl   <- EitherT.fromEither[IO](s"${payload.url}/${payload.filename}.${payload.extension}".asRight)
      website       =   new URL(completeUrl)
      rbc           <- EitherT.fromEither[IO](newChannelE(payload.filename,website))
      filePath      = s"${config.storagePath}/${payload.fileId}"
      fos           = new FileOutputStream(filePath)
      transferred   <- transferE(payload.filename,fos,rbc)
      _             <- Logger.eitherTLogger[IO,Failure].debug(s"SAVE_FILE_DONE ${payload.id} $transferred")
      file          <- EitherT.fromEither[IO](Right(new File(filePath)))
    } yield file
  }
  def saveReplica(payload:Payloads.Replication): EitherT[IO, Failure, Unit] = for {
    _             <- Logger.eitherTLogger[IO,Failure].debug(s"SAVE_REPLICA_INIT ${payload.id}")
    completeUrl   <- EitherT.fromEither[IO](s"${payload.url}/${payload.fileId}.${payload.extension}".asRight)
    website       =   new URL(completeUrl)
    rbc           <- EitherT.fromEither[IO](newChannelE(payload.fileId,website))
    filePath      = s"${config.storagePath}/${payload.fileId}.${payload.extension}"
    fos           = new FileOutputStream(filePath)
    transferred   <- transferE(payload.fileId,fos,rbc)
    _             <- Logger.eitherTLogger[IO,Failure].debug(s"SAVE_REPLICA_DONE ${payload.id} $transferred")
//    file          <- EitherT.fromEither[IO](Right(new File(filePath)))
  } yield ()


  def decompressE(src:String,destination:String): Either[Failure, DecompressionStats] = {
    Either.fromTry(lz4Decompress(src, destination))
    .leftFlatMap(t=>Left(DecompressionFail(t.getMessage)))
  }
  def decompressEIO(src:String,destination:String): EitherT[IO, Failure, DecompressionStats] =
    EitherT.fromEither[IO](decompressE(src,destination))
  def compressE(src:String,destination:String):Either[Failure,CompressionStats] =
    Either.fromTry(lz4Compress(src,destination))
      .leftFlatMap(t=>Left(CompressionFail(t.getMessage)))
  def compressEIO(src:String,destination:String): EitherT[IO, Failure, CompressionStats] =
    EitherT.fromEither[IO](this.compressE(src,destination))


  def saveFile(filename:String, url:String, position:Int=0): IO[File] =
    for {
      _         <- Logger[IO].debug("SAVE_FILE")
       website  =   new URL(url)
       rbc      = Channels.newChannel(website.openStream)
       filePath = s"${config.storagePath}/$filename"
       fos      = new FileOutputStream(filePath)
      _        <- IO.delay(fos.getChannel.transferFrom(rbc,position,Long.MaxValue))
      file     <- IO.pure(new File(filePath))
    } yield  file
}

object Helpers {
  def apply()(implicit utils: RabbitMQUtils[IO],config: DefaultConfig,logger: Logger[IO]) = new Helpers()
}
