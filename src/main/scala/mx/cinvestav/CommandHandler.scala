package mx.cinvestav
//{
//  "commandId":"UPLOAD_FILE",
//  "payload":{
//  "id":"0",
//  "fileId":"f00",
//  "userId":"u00",
//  "filename":"01",
//  "extension":"jpg",
//  "url":"http://localhost:6666",
//  "replicas":2,
//  "nodes":[]
//}
//}
import cats.data.EitherT
import cats.implicits._
import cats.effect.{IO, Ref}
import fs2.concurrent.SignallingRef
import mx.cinvestav.domain.Errors.Failure
import mx.cinvestav.domain.{CommandId, Errors, FileMetadata, Replica}
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

  def uploadFile(command: Command[Json],state:Ref[IO,NodeState])(implicit H:Helpers,config: DefaultConfig,
                                                logger:Logger[IO]): IO[Unit] = command
    .payload.as[Payloads.UploadFile] match {
    case Left(e) =>
      IO.println(e.getMessage())
    case Right(payload) =>
      val getPayloadLog =
        CommandId.UPLOAD_FILE+s" ${payload.id} ${payload.fileId} ${payload.userId} ${payload.url} ${payload.replicas} " +
          s"${payload
            .nodes.mkString(",")}"
      val result:EitherT[IO,Failure,FileMetadata] = for {
        _            <- Logger.eitherTLogger[IO,Failure].debug(getPayloadLog)
        file         <- H.saveFileE(payload)
        currentState <- EitherT(state.get.map(_.asRight[Failure]))
        maybeMeta    <- EitherT.fromEither[IO](currentState.metadata.get(payload.fileId).asRight[Failure])
        metadata     <- H.createFileMetadataE(maybeMeta,payload,file)
//        _            <- Logger.eitherTLogger[IO,Failure].debug(metadata.toString)
//        _            <- EitherT(Logger[IO].debug(metadata.toString).map(_.asRight[Throwable]))
      } yield metadata
      result.value.flatMap {
        case Left(e) => e match {
          case Errors.DuplicatedReplica(fileId) =>
            Logger[IO].error(s"DUPLICATED_REPLICA $fileId")
          case Errors.FileNotFound(filename) =>
            Logger[IO].error(s"FILE_NOT_FOUND $filename")
          case _ =>
            Logger[IO].error("UNKNOWN_ERROR")
        }
        case Right(metadata) =>
                for {
                  currentState   <- state.updateAndGet(s=>s.copy(metadata = s.metadata+(payload.fileId->metadata)))
                  lb             <-  currentState.loadBalancer.pure[IO]
                  storageNodes   <- currentState.storagesNodes.pure[IO]
                  availableNodes <- storageNodes.toSet.diff(payload.nodes.toSet).toList.pure[IO]
                  _              <- Logger[IO].debug(availableNodes.mkString(","))
                  nextNodeId     <- lb.balance(availableNodes).pure[IO]
//                  _              <- Logger[IO].debug(s"SEND_UPLOAD_FILE => $nextNodeId")
                  _              <- if(payload.replicas==0) Logger[IO].debug(s"REPLICATION_FINISHED ${payload.id}")
                                  else H.continueReplication(nextNodeId,payload)
                } yield ( )

      }
//      H.saveFileE(payload.filename,url = s"${payload.url}/${payload.filename}.${payload.extension}")
//      .value.flatMap {
//        case Left(e) =>
//          Logger[IO].error(e.getMessage)
//        case Right(file) =>

//              for {
//                currentState  <- state.get
//                previousMeta  <- currentState.metadata.get(payload.fileId).pure[IO]
//                fileMeta      <- H.createFileMetadata(previousMeta,payload,file)
//                _             <- state.update(s=>s.copy(metadata = s.metadata+(payload.fileId->fileMeta)))
//                _             <- Logger[IO].debug(fileMeta.toString)
//                _             <- Logger[IO].debug(getPayloadLog)
//                lb            <-  currentState.loadBalancer.pure[IO]
//                storageNodes  <- currentState.storagesNodes.pure[IO]
//                availableNodes<- storageNodes.toSet.diff(payload.nodes.toSet).toList.pure[IO]
//                _             <- Logger[IO].debug(availableNodes.mkString(","))
//                nextNodeId    <- lb.balance(availableNodes).pure[IO]
//                _             <- Logger[IO].debug(s"SEND_UPLOAD_FILE => $nextNodeId")
//                _             <- if(payload.replicas==0) Logger[IO].debug("REPLICATION_FINISHED")
//                                else H.continueReplication(nextNodeId,payload)
//              } yield ( )
//      }
  }

  def downloadFile(command:Command[Json])(implicit H:Helpers): IO[Unit] ={
    val payload = command.payload.as[Payloads.DownloadFile]
    payload match {
      case Left(e) =>
        IO.println(e.getMessage())
      case Right(payload) =>
        H.saveFile(payload.fileId,payload.url) *> IO.unit
    }
  }

}
