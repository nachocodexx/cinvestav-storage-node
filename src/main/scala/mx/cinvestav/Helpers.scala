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
import mx.cinvestav.domain.Errors.{DuplicatedReplica, Failure, FileNotFound}
import mx.cinvestav.commons.commands.CommandData
import mx.cinvestav.commons.payloads
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.domain.{CommandId, FileMetadata, Payloads, Replica}
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


class Helpers()(implicit utils: RabbitMQUtils[IO],config: DefaultConfig,logger: Logger[IO]){

  def createFileMetadataE(previousMeta:Option[FileMetadata],payload: Payloads.UploadFile,file: File):EitherT[IO, Failure,
    FileMetadata]
  = previousMeta match {
    case Some(value) =>  EitherT.fromEither[IO](Left( DuplicatedReplica(payload.fileId)))
    case None =>
      val fileMetadata:EitherT[IO,Failure,FileMetadata] = for {
        timestamp     <- EitherT(IO.realTime.map(x=>(x.toSeconds/1000L).asRight))
        replica       <- EitherT(
          IO.pure(Replica(config.nodeId,payload.nodes.isEmpty,0,timestamp).asRight)
        )
        fileMetadata  <- EitherT[IO,Failure,FileMetadata](
          FileMetadata(
          originalName = payload.filename,
          originalExtension = payload.extension,
          compressionAlgorithm = "LZ4",
          size = file.length(),
          replicas = replica::Nil
        ).asRight[Failure].pure[IO]
        )
      } yield fileMetadata
      fileMetadata
  }
  def createFileMetadata(previousMeta:Option[FileMetadata],payload: Payloads.UploadFile,file: File):IO[FileMetadata]
  = previousMeta match {
    case Some(value) =>  IO.raiseError(new Throwable("FILE ALREADY EXISTS"))
    case None =>
      for {
        timestamp     <- IO.realTime.map(_.toSeconds/1000L)
        replica       <- Replica(config.nodeId,payload.nodes.isEmpty,0,timestamp).pure[IO]
        fileMetadata  <- FileMetadata(
          originalName = payload.filename,
          originalExtension = payload.extension,
          compressionAlgorithm = "LZ4",
          size = file.length(),
          replicas = replica::Nil
        ).pure[IO]
      } yield fileMetadata
  }

  def continueReplication(nodeId:String,payload: Payloads.UploadFile): IO[Unit] = for {
    uploadPayload    <- Payloads.UploadFile(
      id       = payload.id,
      fileId   = payload.fileId,
      userId   = payload.userId,
      url      = payload.url,
      replicas = payload.replicas-1,
      nodes    = payload.nodes:+ nodeId,
      filename = payload.filename,
      extension = payload.extension
    ).pure[IO]
    publisher <- fromNodeIdToPublisher(nodeId,s"${config.poolId}.$nodeId.default")
    cmd       <- CommandData[Json](CommandId.UPLOAD_FILE,uploadPayload.asJson).pure[IO]
    _         <- publisher.publish(cmd.asJson.noSpaces)
    _         <- Logger[IO].debug(s"SEND_FILE ${payload.id} $nodeId")
  } yield ()

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
      _        <- Logger[IO].debug(s"HEARTBEAT ${beat.value} ${config.nodeId}")
    } yield ()


  def transferE(payload: Payloads.UploadFile, fos:FileOutputStream,rbc:ReadableByteChannel): EitherT[IO, Failure, Long] =
    EitherT(
      fos.getChannel.transferFrom(rbc,0,Long.MaxValue)
        .pure[IO]
        .map(_.asRight[Failure])
        .handleErrorWith{ t =>
          val res:Either[Failure,Long] = FileNotFound(payload.filename).asLeft[Long]
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
      _            <- Logger.eitherTLogger[IO,Failure].debug(s"SAVE_FILE_INIT ${payload.id}")
      completeUrl  <- EitherT.fromEither[IO](s"${payload.url}/${payload.filename}.${payload.extension}".asRight)
      website       =   new URL(completeUrl)
      rbc          <- EitherT.fromEither[IO](newChannelE(payload.filename,website))
      filePath = s"${config.storagePath}/${payload.filename}"
      fos      = new FileOutputStream(filePath)
      transferred <- transferE(payload,fos,rbc)
      _            <- Logger.eitherTLogger[IO,Failure].debug(s"SAVE_FILE_DONE ${payload.id} $transferred")
      file     <- EitherT.fromEither[IO](Right(new File(filePath)))
    } yield file
//    val result:IO[Either[Throwable,File]] = for {
//      _         <- Logger[IO].debug("SAVE_FILE")
//      website  =   new URL(url)
//      rbc      = Channels.newChannel(website.openStream)
//      filePath = s"${config.storagePath}/$filename"
//      fos      = new FileOutputStream(filePath)
//      _        <- IO.delay(fos.getChannel.transferFrom(rbc,position,Long.MaxValue))
//      file     <- IO.pure(Right(new File(filePath)))
//    } yield  file
//
//    EitherT(result)
  }


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
