package mx.cinvestav
import cats.data.EitherT

import java.io.{File, FileOutputStream}
import java.net.URL
import java.nio.channels.Channels
import java.nio.channels.ReadableByteChannel
import cats.implicits._
import cats.effect.{IO, Ref}
import dev.profunktor.fs2rabbit.model.ExchangeType
import fs2.concurrent.SignallingRef
import mx.cinvestav.domain.Errors.{CompressionFail, DecompressionFail, DuplicatedReplica, Failure, FileNotFound, RFGreaterThanAR}
import mx.cinvestav.commons.commands.CommandData
import mx.cinvestav.commons.payloads
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.domain.Constants.CompressionUtils
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

  def addReplica(fileId:String,replica: Replica,state:Ref[IO,NodeState]): IO[Unit] = for {
    oldMetadata <- state.get.map(_.metadata)
    maybeFileMetadata = oldMetadata.get(fileId)
    _                 <- if(maybeFileMetadata.isDefined) for {
      _            <- IO.unit
      fileMetadata = maybeFileMetadata.get
      replicas     = fileMetadata.replicas :+ replica
      newMetadata  = fileMetadata.copy(replicas = replicas )
      _            <- state.update(s=> s.copy(metadata =   oldMetadata.updated(fileId, newMetadata)   ))
    } yield ()
    else Logger[IO].error(FileNotFound(fileId).message)
//    _           <- if(maybeFileMetadata.isDefined) IO.unit else IO.unit
  } yield ()

  def activeReplication(payload:Payloads.UploadFile, metadata: FileMetadata,currentState:NodeState): IO[Unit] = for {
//    currentState <-
     _         <- Logger[IO].info(s"ACTIVE_REPLICATION ${payload.id} ${payload.fileId}")
     loadBalancer <-  currentState.loadBalancer.pure[IO]
     storageNodes <- currentState.storagesNodes.pure[IO]
     _            <- if(storageNodes.length < payload.replicationFactor)
                                Logger[IO].error(RFGreaterThanAR().message)
              else for {
                selectedStorageNodes <- loadBalancer.balanceMulti(storageNodes,rounds=currentState.replicationFactor).pure[IO]
                publishers <- selectedStorageNodes.traverse(nId =>
                  utils.fromNodeIdToPublisher(
                    nodeId       = nId,
                    exchangeName = config.poolId,
                    routingKey   = s"${config.poolId}.$nId.default"
                  )
                )
                compressionExt = CompressionUtils.getExtensionByCompressionAlgorithm(payload.compressionAlgorithm)
                _payload = Payloads.ActiveReplication(
                    id           = payload.id,
                    metadata     =  metadata,
                    userId       = payload.userId,
                    fileId       = payload.fileId,
                    url          = s"http://${currentState.ip}/${payload.fileId}.$compressionExt",
                    leaderNodeId = config.nodeId
                  )
                cmd      <- CommandData[Json](CommandId.ACTIVE_REPLICATION,_payload.asJson).pure[IO].map(_.asJson.noSpaces)
                _        <- publishers.traverse{publisher=>
                  publisher.publish(cmd) *> Logger[IO].debug(s"SENT REPLICA TO ${publisher.nodeId} SUCCESSFULLY")
                }
              } yield ()
  } yield ()

  def saveAndCompress(payload: Payloads.UploadFile, maybeMeta:Option[FileMetadata])(implicit H:Helpers,
                                                                                    config: DefaultConfig,
                                                                                    logger: Logger[IO]):EitherT[IO, Failure, FileMetadata] =
    if(maybeMeta.isDefined) EitherT.fromEither[IO](Left(DuplicatedReplica(payload.fileId)))
    else for {
      file             <- H.saveFileE(payload)
      _                <-Logger.eitherTLogger[IO,Failure].debug(s"COMPRESSION_INIT ${payload.id} ${payload.fileId} " +
        s"${payload.experimentId}")
      cs               <- H.compressEIO(file.getPath,s"${config.storagePath}")
      _                <- Logger.eitherTLogger[IO,Failure]
        .debug(s"COMPRESSION_DONE ${payload.id} ${payload.fileId} ${cs.method} ${cs.millSeconds} ${cs.sizeIn} ${cs
          .sizeOut} ${cs.mbPerSecond} ${payload.experimentId}")
      metadata         <- H.createFileMetadataE(payload,file)
      _                <- EitherT.fromEither[IO](file.delete().asRight)
    } yield  metadata

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

  def passiveReplication(currentState:NodeState, payload: Payloads.Replication): IO[Unit] = for {
    lb             <-  currentState.loadBalancer.pure[IO]
    storageNodes   <- currentState.storagesNodes.pure[IO]
    availableNodes <- storageNodes.toSet.diff(payload.nodes.filter(_!=config.nodeId).toSet).toList.pure[IO]
//
    nodeId         <- lb.balance(availableNodes).pure[IO]
    publisher      <- utils.fromNodeIdToPublisher(nodeId,config.poolId,s"${config.poolId}.$nodeId.default")
    cmd            <- CommandData[Json](CommandId.REPLICATION,payload.asJson).pure[IO]
    _              <- publisher.publish(cmd.asJson.noSpaces)
    _              <- Logger[IO].debug(s"SENT_REPLICATION_CMD ${payload.id} $nodeId ${payload.experimentId}")

  } yield ()




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

  def downloadFileFormURL(fileId:String,filePath:String,url: URL): EitherT[IO, Failure, Long] = for {
    rbc           <- EitherT.fromEither[IO](newChannelE(fileId,url))
    fos           = new FileOutputStream(filePath)
//    _ <- url.getPath
    transferred   <- transferE(fileId,fos,rbc)
  } yield transferred
  def saveFileE(payload:Payloads.UploadFile): EitherT[IO,Failure,File] = {
    for {
      _             <- Logger.eitherTLogger[IO,Failure].debug(s"SAVE_FILE_INIT ${payload.id} ${payload.fileId} " +
        s"${payload.experimentId}")
      completeUrl   <- EitherT.fromEither[IO](s"${payload.url}/${payload.filename}.${payload.extension}".asRight)
      website       =   new URL(completeUrl)
      filePath      = s"${config.storagePath}/${payload.fileId}"
      transferred <- downloadFileFormURL(payload.fileId,filePath,website)
//      rbc           <- EitherT.fromEither[IO](newChannelE(payload.filename,website))
//      filePath      = s"${config.storagePath}/${payload.fileId}"
//      fos           = new FileOutputStream(filePath)
//      transferred   <- transferE(payload.filename,fos,rbc)
      _             <- Logger.eitherTLogger[IO,Failure].debug(s"SAVE_FILE_DONE ${payload.id} ${payload
        .fileId} $transferred ${payload.experimentId}")
      file          <- EitherT.fromEither[IO](Right(new File(filePath)))
    } yield file
  }
  def saveReplica(payload:Payloads.Replication): EitherT[IO, Failure, Unit] = for {
    _             <- Logger.eitherTLogger[IO,Failure].debug(s"SAVE_REPLICA_INIT ${payload.id} ${payload.fileId} " +
      s"${payload.experimentId}")
    completeUrl   <- EitherT.fromEither[IO](s"${payload.url}/${payload.fileId}.${payload.extension}".asRight)
    website       =   new URL(completeUrl)
    rbc           <- EitherT.fromEither[IO](newChannelE(payload.fileId,website))
    filePath      = s"${config.storagePath}/${payload.fileId}.${payload.extension}"
    fos           = new FileOutputStream(filePath)
    transferred   <- transferE(payload.fileId,fos,rbc)
    _             <- Logger.eitherTLogger[IO,Failure].debug(s"SAVE_REPLICA_DONE ${payload.id} ${payload.fileId} " +
      s"$transferred ${payload.experimentId}")
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
