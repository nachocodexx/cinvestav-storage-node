package mx.cinvestav


import cats.data.EitherT
import cats.implicits._
import cats.effect.{IO, Ref}
import fs2.concurrent.SignallingRef
import mx.cinvestav.commons.status
import mx.cinvestav.domain.Constants.CompressionUtils
import mx.cinvestav.domain.Errors.{DuplicatedReplica, Failure, RFGreaterThanAR}
import mx.cinvestav.domain.{CommandId, Errors, FileMetadata, Replica}

import java.io.File
import java.net.InetAddress
//
import io.circe.generic.semiauto.deriveDecoder
import io.circe.{Decoder, Json}
//
import mx.cinvestav.domain.NodeState
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.commons.payloads
import mx.cinvestav.utils.{Command, RabbitMQUtils}
import mx.cinvestav.commons.commands.Identifiers
//
import org.typelevel.log4cats.Logger
import mx.cinvestav.commons.balancer
import sys.process._
//
import scala.concurrent.duration._
import scala.language.postfixOps

object CommandHandlers {
  implicit val startHeartbeatPayloadDecoder:Decoder[payloads.StartHeartbeat] = deriveDecoder
  implicit val stopHeartbeatPayloadDecoder:Decoder[payloads.StopHeartbeat] = deriveDecoder
  implicit val newCoordinatorPayloadDecoder:Decoder[payloads.NewCoordinator] = deriveDecoder
  implicit val newCoordinatorV2PayloadDecoder:Decoder[payloads.NewCoordinatorV2] = deriveDecoder

  def reset(command: Command[Json],state:Ref[IO,NodeState])(implicit config: DefaultConfig,logger: Logger[IO]): IO[Unit] = for {
    _ <- Logger[IO].debug(s"RESET ${config.nodeId}")
    cmdRm    = s"rm ${config.storagePath}/*.*"
    rootFile = new File("/")
    cmd      = Seq("/bin/sh","-c",cmdRm)
    currentState   <- state.get
    _initState      <- NodeState(
      status              = status.Up,
      heartbeatSignal     = currentState.heartbeatSignal,
      loadBalancer        = balancer.LoadBalancer(config.loadBalancer),
      replicationFactor   = config.replicationFactor,
      storagesNodes       = config.storageNodes,
      ip                  = InetAddress.getLocalHost.getHostAddress,
      availableResources  = config.storageNodes.length+1,
      replicationStrategy =  config.replicationStrategy,
      freeStorageSpace    = rootFile.getFreeSpace,
      usedStorageSpace    =  rootFile.getTotalSpace - rootFile.getFreeSpace,
      chordRoutingKey          = s"${config.poolId}.global.chord"
    ).pure[IO]
    _  <- state.update(_=>_initState)
    _ <- IO.pure(cmd!)
  } yield ()

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

}
