package mx.cinvestav


import cats.data.EitherT
import cats.implicits._
import cats.effect.{IO, Ref}
import fs2.concurrent.SignallingRef
import mx.cinvestav.domain.Errors.{DuplicatedReplica, Failure, RFGreaterThanAR}
import mx.cinvestav.domain.{CommandId, Errors, FileMetadata, Replica}

import java.net.InetAddress
//
import dev.profunktor.fs2rabbit.model.ExchangeType
import io.circe.generic.semiauto.deriveDecoder
import io.circe.{Decoder, Json}
//
import mx.cinvestav.domain.{NodeState, Payloads}
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.domain.Payloads.UpdateReplicationFactor
import mx.cinvestav.commons.payloads
import mx.cinvestav.utils.{Command, RabbitMQUtils}
import mx.cinvestav.commons.commands.Identifiers
//
import org.typelevel.log4cats.Logger
import mx.cinvestav.commons.balancer
//
import scala.concurrent.duration._
import scala.language.postfixOps

object CommandHandler {
  implicit val downloadFilePayloadDecoder:Decoder[Payloads.DownloadFile] = deriveDecoder
  implicit val updateReplicationFactorPayloadDecoder:Decoder[Payloads.UpdateReplicationFactor] = deriveDecoder
  implicit val uploadFilePayloadDecoder:Decoder[Payloads.UploadFile] = deriveDecoder
  implicit val startHeartbeatPayloadDecoder:Decoder[payloads.StartHeartbeat] = deriveDecoder
  implicit val stopHeartbeatPayloadDecoder:Decoder[payloads.StopHeartbeat] = deriveDecoder
  implicit val replicationPayloadDecoder:Decoder[Payloads.Replication] = deriveDecoder
  implicit val newCoordinatorPayloadDecoder:Decoder[payloads.NewCoordinator] = deriveDecoder
  implicit val newCoordinatorV2PayloadDecoder:Decoder[payloads.NewCoordinatorV2] = deriveDecoder

  def newCoordinatorV2(command: Command[Json],state:Ref[IO,NodeState])(implicit logger: Logger[IO]): IO[Unit] =
    command.payload.as[payloads.NewCoordinatorV2] match {
    case Left(value) =>
      Logger[IO].error(value.getMessage())
    case Right(payload) =>for {
      _ <- Logger[IO].debug(s"NEW_COORDINATOR ${payload.prev} ${payload.current}")
      _ <- state.update(s =>
        s.copy(availableResources = s.availableResources-1,storagesNodes = s.storagesNodes.toSet.diff(payload.prev.toSet).toList )
      )
    } yield ()
  }

  def newCoordinator(command: Command[Json],state:Ref[IO,NodeState])(implicit logger: Logger[IO]): IO[Unit] = command.payload
    .as[payloads.NewCoordinator] match {
    case Left(value) =>
      Logger[IO].error(value.getMessage())
    case Right(payload) =>for {
      _ <- Logger[IO].debug(s"NEW_COORDINATOR ${payload.prev} ${payload.current}")
      _ <- state.update(s =>
        s.copy(availableResources = s.availableResources-1,storagesNodes = s.storagesNodes.filter(_!=payload.prev))
      )
    } yield ()
  }

  def replication(command: Command[Json],state:Ref[IO,NodeState])(implicit utils: RabbitMQUtils[IO],
                                                                  config: DefaultConfig,logger: Logger[IO],H:Helpers)
  : IO[Unit] =
    command.payload.as[Payloads.Replication] match {
    case Left(value) =>
      Logger[IO].error(value.getMessage())
    case Right(payload) => for {
      currentState <- state.get
      _                   <- Logger[IO].debug(CommandId.REPLICATION+s" ${payload.id} ${payload.fileId} ${payload.experimentId}")
      replicationCompletedLog =Logger[IO].debug (s"REPLICATION_COMPLETED ${payload.id} ${payload.experimentId}")

      continueReplication <- (payload.nodes.length < payload.replicationFactor).pure[IO]
      replicationFn = for {
        result <- H.saveReplica(payload).value
        _      <- result match {
          case Left(e) => Logger[IO].error(e.toString)
          case Right(_) => for {
            _ <-Logger[IO].debug(s"REPLICA_SAVED_SUCCESSFULLY ${payload.id} ${payload.experimentId}")
            newPayload = payload.copy(url =  s"http://${currentState.ip}",nodes = payload.nodes:+config.nodeId)
            _<- if(continueReplication) H.replicate(currentState, newPayload) else replicationCompletedLog
          } yield ()
        }
//        _ <- H.replicate(currentState,payload)
      } yield ()
      _ <- replicationFn
//      _                   <- if(continueReplication) replicationFn else Logger[IO].debug(s"REPLICATION_DONE ${payload.id}")
    } yield ()
//      Logger[IO].debug("REPLICATION")
  }

  def stopHeartbeat(command: Command[Json],state:Ref[IO,NodeState])(implicit utils:RabbitMQUtils[IO],
                                                                    config: DefaultConfig,logger: Logger[IO]): IO[Unit] = {
    command.payload.as[payloads.StopHeartbeat] match {
      case Left(e) =>
        Logger[IO].error(e.getMessage())
      case Right(payload) =>
        for {
          _               <- Logger[IO].debug(Identifiers.STOP_HEARTBEAT+s" ${payload.id} ${payload.nodeId} ${payload
            .fromNodeId}")
          currentState    <- state.getAndUpdate(_.copy(isBeating = false))
          heartbeatSignal <- currentState.heartbeatSignal.pure[IO]
          _               <- heartbeatSignal.set(true)
          _               <- heartbeatSignal.set(false)
        } yield ()
    }
  }


  def startHeartbeat(command: Command[Json],state:Ref[IO,NodeState])(implicit utils:RabbitMQUtils[IO],
                                                                     H:Helpers,logger: Logger[IO]): IO[Unit] = command
    .payload.as[payloads.StartHeartbeat] match {
    case Left(value) =>
      Logger[IO].error(value.getMessage())
    case Right(payload) =>
      for {
        _               <- Logger[IO].debug(Identifiers.START_HEARTBEAT+s" ${payload.id} ${payload.nodeId} ${payload.fromNodeId}")
        currentState    <- state.get
        heartbeatSignal <- currentState.heartbeatSignal.pure[IO]
        isBeating       <- currentState.isBeating.pure[IO]
        _               <- if(isBeating) Logger[IO].debug("HEARTBEAT_SKIPPED")
                          else H._startHeart(heartbeatSignal) *> state.update(_.copy(isBeating=true))
      } yield ()
  }

  def updateReplicationFactor(command: Command[Json],state:Ref[IO,NodeState]): IO[Unit] = {
    command.payload
      .as[UpdateReplicationFactor] match {
      case Left(e) =>
        IO.println(e.getMessage())
      case Right(payload) =>
        for {
           _ <- IO.println("UPDATE REPLICATION FACTOR")
          _ <- state.update(s=>s.copy(replicationFactor = payload.replicationFactor))
        } yield ()
    }
//    val routingKey =command.envelope.routingKey.value
//    state.getAndUpdate{ nodeState =>
////      val currentRF = nodeState.getOrElse("rf", 1).asInstanceOf[Int]
////      nodeState.updated("rf",currentRF+1)
//    }.flatMap(IO.println)
//      .flatMap(_=>IO.println(command.envelope))
  }


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

  def uploadFile(command: Command[Json],state:Ref[IO,NodeState])(implicit H:Helpers,config: DefaultConfig, logger:Logger[IO]): IO[Unit] = command
    .payload.as[Payloads.UploadFile] match {
    case Left(e) =>
      IO.println(e.getMessage())
    case Right(payload) =>
      val uploadLog = CommandId.UPLOAD_FILE+s" ${payload.id} ${payload.fileId} ${payload.userId} ${payload.url} " +
        s"${payload.replicationFactor} ${payload.experimentId}"
      val result:EitherT[IO,Failure,FileMetadata] = for {
        _                <- Logger.eitherTLogger[IO,Failure].debug(uploadLog)
        currentState     <- EitherT(state.get.map(_.asRight[Failure]))

        metadata = for {
          maybeMeta        <- EitherT.fromEither[IO](currentState.metadata.get(payload.fileId).asRight[Failure])
          metadata         <- saveAndCompress(payload,maybeMeta)
        } yield metadata

       m <- if(currentState.availableResources <= payload.replicationFactor)
         EitherT.fromEither[IO](Either.left[Failure,FileMetadata](RFGreaterThanAR()))
       else metadata
      } yield m


      result.value.flatMap {
        case Left(e) => e match {
          case Errors.DuplicatedReplica(fileId) =>
            Logger[IO].error(s"DUPLICATED_REPLICA $fileId")
          case Errors.FileNotFound(filename) =>
            Logger[IO].error(s"FILE_NOT_FOUND $filename")
          case Errors.CompressionFail(message) =>
            Logger[IO].error(message)
          case RFGreaterThanAR(message) =>
            Logger[IO].error(message)
          case _ =>
            Logger[IO].error("UNKNOWN_ERROR")
        }
        case Right(metadata) =>
                for {
                  currentState   <- state.updateAndGet(s=>s.copy(metadata = s.metadata+(payload.fileId->metadata)))
                  replicationPayload = Payloads.Replication(
                    id = payload.id,
                    fileId = payload.fileId,
                    extension = metadata.compressionExt,
                    userId=payload.userId,
                    url = s"http://${currentState.ip}",
                    originalFilename = metadata.originalName,
                    originalExtension = metadata.originalExtension,
                    originalSize = metadata.size,
                    replicationFactor=payload.replicationFactor,
                    compressionAlgorithm = payload.compressionAlgorithm,
                    nodes= config.nodeId::Nil,
                    experimentId = payload.experimentId
                  )
                  _               <- H.replicate(currentState,replicationPayload)
//                  _              <- if(payload.replicas==0) Logger[IO].debug(s"REPLICATION_FINISHED ${payload.id}")
//                                  else H.continueReplication(currentState,payload)
//                                  else H.continueReplication(currentState,payload)
                } yield ( )

      }
  }

  def downloadFile(command:Command[Json],state:Ref[IO,NodeState])(implicit H:Helpers,logger: Logger[IO]): IO[Unit] ={
    val payload = command.payload.as[Payloads.DownloadFile]
    payload match {
      case Left(e) =>
        IO.println(e.getMessage())
      case Right(payload) =>
        for {
          currentState  <- state.get
          maybeMetadata <- currentState.metadata.get(payload.fileId).pure[IO]
          successLog         = Logger[IO].debug(CommandId.DOWNLOAD_FILE+s" ${payload.fileId}")
          errorLog           = Logger[IO].error(s"FILE_NOT_FOUND ${payload.fileId}")
//          ipLog              = Logger[IO].debug()
          _            <- if(maybeMetadata.isDefined)
                            successLog  *> Logger[IO].debug(maybeMetadata.get.toString)
                          else errorLog
//          _            <- ipLog
        } yield ()
//        H.saveFile(payload.fileId,payload.url) *> IO.unit
    }
  }

}
