package mx.cinvestav


import cats.data.EitherT
import cats.implicits._
import cats.effect.{IO, Ref}
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.{BooleanVal, StringVal}
import dev.profunktor.fs2rabbit.model.{AmqpEnvelope, AmqpMessage, AmqpProperties}
import fs2.concurrent.SignallingRef
import mx.cinvestav.Declarations.{NodeContextV5, NodeError, NodeStateV5, UploadFileOutput, liftFF}
import mx.cinvestav.commons.status
import mx.cinvestav.commons.storage.{FileMetadata, Replica}
import mx.cinvestav.domain.Constants.ReplicationStrategies
import mx.cinvestav.domain.Errors.{Failure, RFGreaterThanAR}
import mx.cinvestav.domain.Payloads
import mx.cinvestav.utils.v2.{Acker, processMessage}
import org.apache.commons.io.FileUtils

import java.io.File
import java.net.InetAddress
import java.nio.file.Paths
//
import io.circe.generic.semiauto.deriveDecoder
import io.circe.{Decoder, Json}
import io.circe.generic.auto._
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
import mx.cinvestav.utils.v2.encoders._
import mx.cinvestav.commons.stopwatch.StopWatch._

object CommandHandlers {
  implicit val startHeartbeatPayloadDecoder:Decoder[payloads.StartHeartbeat] = deriveDecoder
  implicit val stopHeartbeatPayloadDecoder:Decoder[payloads.StopHeartbeat] = deriveDecoder
  implicit val newCoordinatorPayloadDecoder:Decoder[payloads.NewCoordinator] = deriveDecoder
  implicit val newCoordinatorV2PayloadDecoder:Decoder[payloads.NewCoordinatorV2] = deriveDecoder


  def uploadV5()(implicit ctx:NodeContextV5,envelope: AmqpEnvelope[String],acker:Acker): IO[Unit] = {
    def successCb(acker: Acker,envelope: AmqpEnvelope[String],payload:Payloads.UploadFileV5) = {
      ctx.logger.info(payload.toString)
      type E                = NodeError
      val maybeCurrentState = EitherT.liftF[IO,E,NodeStateV5](ctx.state.get)
//      val maybeSlaveFlag    = EitherT.fromOption[IO](envelope.properties.headers.get("isSlave"))
      implicit val logger   = ctx.logger
      val L                 = Logger.eitherTLogger[IO,E]
      val app = for {
        currentState <- maybeCurrentState
        isSlave      = envelope.properties.headers.getOrElse("isSlave",BooleanVal(false)).toValueWriterCompatibleJava.asInstanceOf[Boolean]
        sourceFolder = ctx.config.sourceFolders.head
        sourceFolderPath = Paths.get(sourceFolder).resolve(payload.fileId)
        sinkFolder   = ctx.config.storagePath
        sinkFolderPath = Paths.get(sinkFolder).resolve(payload.fileId)
        source       = sourceFolderPath.toFile
        sink         = sinkFolderPath.toFile
        _ <- L.info(sourceFolderPath.toString)
        _ <- L.info(sinkFolderPath.toString)
        _ <- L.info("IS_SLAVE: "+isSlave)
        _ <- liftFF[Unit](IO.pure(FileUtils.copyDirectory(source,sink)))
      } yield UploadFileOutput(sink,isSlave)

      app.value.stopwatch.flatMap { result =>
        result.result match {
          case Left(e) =>
            acker.reject(deliveryTag =envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
          case Right(uploadFileOutput) => for {
            currentState <- ctx.state.get
            replicationStrategy = currentState.replicationStrategy
            _            <- acker.ack(deliveryTag = envelope.deliveryTag)
            _            <- ctx.logger.info(s"UPLOAD_FILE ${payload.fileId} ${result.duration}")
            _ <- if(replicationStrategy == ReplicationStrategies.ACTIVE && !uploadFileOutput.isSlave)
              Helpers.activeReplicationV5(
                payload =  payload.copy(source = uploadFileOutput.sink.toPath.toString),
                storageNodes = currentState.storageNodesPubs
                  .filter(_._1!=ctx.config.nodeId)
                  .values
                  .toList
                  .take(payload.replicationFactor)
              )
            else if(replicationStrategy == ReplicationStrategies.PASSIVE) Helpers.passiveReplicationV5(payload,Nil)
            else ctx.logger.info(s"REPLICATION_DONE ${payload.fileId}")
          } yield ()
        }
      }
    }
    processMessage[IO,Payloads.UploadFileV5,NodeContextV5](
      successCallback = successCb,
      errorCallback =  (acker,envelope,e)=>ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
    )
  }
// ____________________________________________________________________________________________________________

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
      chordRoutingKey     = s"${config.poolId}.global.chord",
      metadata            = Map.empty[String,FileMetadata]
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
