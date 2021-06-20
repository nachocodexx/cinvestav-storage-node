package mx.cinvestav

import cats.implicits._
import cats.effect.{IO, Ref}
import fs2.concurrent.SignallingRef
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
  implicit val downloadFilePayloadDecoder:Decoder[Payloads.DownloadFilePayload] = deriveDecoder
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
                                                                     config:DefaultConfig,logger: Logger[IO]): IO[Unit] = command.payload.as[payloads.StartHeartbeat] match {
    case Left(value) =>
      Logger[IO].error(value.getMessage())
    case Right(payload) =>
      for {
        _               <- Logger[IO].debug(Identifiers.START_HEARTBEAT+s" ${payload.id} ${payload.nodeId} ${payload.fromNodeId}")
        currentState    <- state.get
        heartbeatSignal <- currentState.heartbeatSignal.pure[IO]
        isBeating       <- currentState.isBeating.pure[IO]
        _               <- if(isBeating) Logger[IO].debug("HEARTBEAT_SKIPPED")
                          else Helpers._startHeart(heartbeatSignal) *> state.update(_.copy(isBeating=true))
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

  def uploadFile(command: Command[Json],state:Ref[IO,NodeState])(implicit utils:RabbitMQUtils[IO],config: DefaultConfig,
                                                logger:Logger[IO]): IO[Unit] = command
    .payload.as[Payloads.UploadFile] match {
    case Left(e) =>
      IO.println(e.getMessage())
    case Right(payload) =>
      for {
        currentState <- state.get
        _        <- Logger[IO].debug(s"UPLOADING FILE  - DOWNLOAD FROM ${payload.url}")
        lb       <-  currentState.loadBalancer.pure[IO]
        replicas <- lb.balance(currentState.storagesNodes).pure[IO]
        _        <- Logger[IO].debug(s"REPLICAS: ${replicas}")
//        _ <-
      } yield ( )
  }

  def downloadFile(command:Command[Json])(implicit config:DefaultConfig): IO[Unit] ={
    val payload = command.payload.as[Payloads.DownloadFilePayload]
    payload match {
      case Left(e) =>
        IO.println(e.getMessage())
      case Right(payload) =>
        Helpers.saveFile(payload.fileId,payload.url) *> IO.unit
    }
  }

}
