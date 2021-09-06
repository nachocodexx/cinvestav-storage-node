package mx.cinvestav


import cats.data.EitherT
import cats.implicits._
import cats.effect.{IO, Ref}
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.{ArrayVal, BooleanVal, StringVal}
import dev.profunktor.fs2rabbit.model.{AmqpEnvelope, AmqpFieldValue, AmqpMessage, AmqpProperties, ExchangeName, RoutingKey}
import fs2.concurrent.SignallingRef
import mx.cinvestav.Declarations.{BadArguments, NodeContextV5, NodeError, NodeStateV5, StorageNode, UploadFileOutput, liftFF}
import mx.cinvestav.commons.status
import mx.cinvestav.commons.storage.{FileMetadata, Replica}
import mx.cinvestav.commons.fileX.{FileMetadata => Metadata}
import mx.cinvestav.commons.types.Location
import mx.cinvestav.domain.Constants.ReplicationStrategies
import mx.cinvestav.domain.Errors.{Failure, RFGreaterThanAR}
import mx.cinvestav.domain.Payloads
import mx.cinvestav.server.Client
import mx.cinvestav.utils.v2.{Acker, PublisherConfig, PublisherV2, processMessage, processMessageV2}
import org.apache.commons.io.FileUtils

import java.io.File
import java.net.{InetAddress, URL}
import java.nio.file.Paths
//import scala.sys.process.processInternal.URL
//
import io.circe.generic.semiauto.deriveDecoder
import io.circe.{Decoder, Json}
import io.circe.generic.auto._
//
import mx.cinvestav.domain.NodeState
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.commons.payloads
import mx.cinvestav.commons.payloads.{v2=>PAYLOADS}
import mx.cinvestav.utils.{Command, RabbitMQUtils}
import mx.cinvestav.commons.commands.Identifiers
//
import org.typelevel.log4cats.Logger
import mx.cinvestav.commons.balancer
import sys.process._
import io.circe.generic.auto._
import io.circe.syntax._
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


  def addStorageNode()(implicit ctx:NodeContextV5,envelope: AmqpEnvelope[String],acker:Acker):IO[Unit] = {
    def successCallback(acker:Acker,envelope: AmqpEnvelope[String],payload: Payloads.AddStorageNode):IO[Unit] = {
      type E                = NodeError
      val maybeCurrentState = EitherT.liftF[IO,E,NodeStateV5](ctx.state.get)
      implicit val logger   = ctx.logger
      val L                 = Logger.eitherTLogger[IO,E]
      implicit val rabbitMQContext = ctx.rabbitMQContext
      val app = for {
        _ <- EitherT.fromEither[IO](Either.unit[NodeError])
        predicate = payload.storageNodeId!=ctx.config.nodeId
        check = Option.when(predicate)(())
        _ <- L.info(check.toString+s" ${payload.storageNodeId} != ${ctx.config.nodeId} -> $predicate")
        _ <- EitherT.fromEither[IO](Either.fromOption(check,  BadArguments("Storage node cannot add itself")  ))
        storageNodeId   = payload.storageNodeId
        poolId          = ctx.config.poolId
        nodeId          = ctx.config.nodeId
        routingKey      = RoutingKey(s"$poolId.$nodeId")
        exchangeName    = ExchangeName(poolId)
        publisherConfig = PublisherConfig(exchangeName=exchangeName,routingKey=routingKey)
        publisher       = PublisherV2(publisherConfig = publisherConfig)
//          ctx.rabbitContext.client.createPublisher(exchangeName = ???,routingKey = ???)
        currentState <- liftFF[NodeStateV5](ctx.state.updateAndGet(
          s=>s.copy(
            storagesNodes      = s.storagesNodes :+ storageNodeId,
            storageNodesPubs   = s.storageNodesPubs + (storageNodeId -> publisher),
            availableResources = s.availableResources+1
          )
        )
        )
        _ <- L.debug(currentState.toString)
      } yield()
//    __________________________________________________________________
      app.value.stopwatch.flatMap { result =>
        result.result match {
          case Left(e) =>  acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
          case Right(value) =>
            acker.ack(deliveryTag = envelope.deliveryTag) *> ctx.logger.info(s"ADD_STORAGE_NODE ${payload.storageNodeId}")
        }
      }
    }

    processMessage[IO,Payloads.AddStorageNode,NodeContextV5](
      successCallback =  successCallback,
      errorCallback   =  (acker,envelope,e)=>ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
    )
  }
  def removeStorageNode()(implicit ctx:NodeContextV5,envelope: AmqpEnvelope[String],acker:Acker):IO[Unit] = {
    def successCallback(acker:Acker,envelope: AmqpEnvelope[String],payload: Payloads.RemoveStorageNode):IO[Unit] = {
      type E                = NodeError
      val maybeCurrentState = EitherT.liftF[IO,E,NodeStateV5](ctx.state.get)
      implicit val logger   = ctx.logger
      val L                 = Logger.eitherTLogger[IO,E]
      val app = for {
        _ <- EitherT.fromEither[IO](Either.unit[NodeError])
//      _________________________________________________________________________________________
        nodeId        = ctx.config.nodeId
        storageNodeId = payload.storageNodeId
        predicate     = payload.storageNodeId!=ctx.config.nodeId
        check         = Option.when(predicate)(())
        error         = BadArguments("Storage node cannot remove itself")
//      _________________________________________________________________________________
        _ <- L.debug(check.toString+s" $storageNodeId != $nodeId -> $predicate")
        _ <- EitherT.fromEither[IO](Either.fromOption(check,error))
//      ________________________________________________________________________________
        currentState <- liftFF[NodeStateV5](ctx.state.updateAndGet(
          s=>s.copy(
            storagesNodes      = s.storagesNodes.filter(_!=storageNodeId),
            storageNodesPubs   = s.storageNodesPubs.filter(_._1!=storageNodeId),
            availableResources = s.availableResources-1
          )
        )
        )
//      ________________________________________________________________________________
        _ <- L.debug(currentState.toString)
      } yield()
      app.value.stopwatch.flatMap { result =>
        result.result match {
          case Left(e) =>  acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
          case Right(_) => for {
            _ <-acker.ack(deliveryTag = envelope.deliveryTag)
            storageNodeId = payload.storageNodeId
            _ <- ctx.logger.info(s"REMOVE_STORAGE_NODE $storageNodeId")
          } yield ()
        }
      }
    }
    processMessage[IO,Payloads.RemoveStorageNode,NodeContextV5](
      successCallback =  successCallback,
      errorCallback   =  (acker,envelope,e)=>ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
    )
  }

  def uploadV5()(implicit ctx:NodeContextV5,envelope: AmqpEnvelope[String],acker:Acker): IO[Unit] = {
    def successCb(payload:Payloads.UploadV5) = {
      type E                = NodeError
      val maybeCurrentState = EitherT.liftF[IO,E,NodeStateV5](ctx.state.get)
      implicit val rabbitMQContext = ctx.rabbitMQContext
      val storageNodes      = List.from(envelope.properties.headers
        .getOrElse("storageNodes", ArrayVal(Vector.empty[AmqpFieldValue])  )
        .asInstanceOf[ArrayVal]
        .toValueWriterCompatibleJava.toArray)
        .map(_.toString)

      val storageNodesPubs = storageNodes.map(_.split('.'))
        .map(x=>(x(0),x(1)))
        .map(StorageNode.tupled)
        .map(Helpers.fromStorageNodeToPublisher)
      implicit val logger   = ctx.logger
      val L                 = Logger.eitherTLogger[IO,E]
      val app = for {
//      CURRENT_STATE
        currentState     <- maybeCurrentState
//       NODE_ID
        nodeId           = ctx.config.nodeId
//       POOL_ID
        poolId           = ctx.config.poolId
//      FILE_ID
        fileId           = payload.id
//      RESOURCE URL
        fileUrl          = payload.url
//       SOURCE
        source           = payload.source
        sourcePath       = Paths.get(source)
        metadata         = Metadata.fromPath(sourcePath)
//      _______________________________________________________________
        isSlave          = envelope.properties.headers.getOrElse("isSlave",BooleanVal(false)).toValueWriterCompatibleJava.asInstanceOf[Boolean]
        sinkFolder       = s"${ctx.config.storagePath}/${poolId}_$nodeId"
        sinkFolderPath   = Paths.get(sinkFolder)
        _                <- liftFF[Unit](IO.delay{sinkFolderPath.toFile.mkdirs()}.void)
        destination      = sinkFolderPath.resolve(metadata.fullname)
//      ______________________________________________________
        _                <- L.debug(s"STORAGE_NODES ${storageNodes.mkString("/")}")
        _                <- L.debug(s"STORAGE_POOLS_NODES ${storageNodesPubs.map(_.pubId)}")
        _                <- L.debug("FILE_ID "+fileId)
        _                <- L.debug("URL "+fileUrl)
        _                <- L.debug("SOURCE "+source)
        _                <- L.debug("SINK "+destination.toString)
        _                <- L.debug("IS_SLAVE "+isSlave)
        _                <- L.debug("METADATA "+metadata.toString)
//      DOWNLOAD OR COPY FILE
        x <- liftFF[Unit](
          Client
            .downloadFile(fileUrl,sourcePath = source,destination = destination)
            .handleError(x=>ctx.logger.error(x.getMessage))
        )
        _ <- L.debug(s"DOWNLOAD_COMPLETED")
//      ______________________________________________________
      } yield UploadFileOutput(sink = destination.toFile,isSlave = isSlave,metadata = metadata)
//    ________________________________________________________________________________________
      app.value.stopwatch.flatMap { result =>
        result.result match {
          case Left(e) =>
            acker.reject(deliveryTag =envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
          case Right(uploadFileOutput) => for {
//          __________________________________________________________________________________________
            _                   <- acker.ack(deliveryTag = envelope.deliveryTag)
            timestamp           <- IO.realTime.map(_.toMillis)
            currentState        <- ctx.state.get
            nodeId              = ctx.config.nodeId
            poolId              = ctx.config.poolId
            lbExchange          = ExchangeName(ctx.config.loadBalancer.exchange)
            lbRk                = RoutingKey(ctx.config.loadBalancer.routingKey)
            lbPubCfg            = PublisherConfig(exchangeName = lbExchange,routingKey = lbRk)
            lbPub               = PublisherV2(lbPubCfg)
            replicationStrategy = currentState.replicationStrategy
            url                 = s"http://${currentState.ip}:${ctx.config.port}/download"
            source              = uploadFileOutput.sink.toPath
            taskId              = payload.id
            chunkId             = uploadFileOutput.metadata.filename.value
            location            = Location(url =url,source =source.toString)
            _                   <- ctx.logger.info(s"UPLOAD_FILE ${payload.id} $poolId ${uploadFileOutput.sink.length()} ${result.duration.toMillis}")

//          _________________________________________________________________________________________
//            _ <- if(replicationStrategy == ReplicationStrategies.ACTIVE && !uploadFileOutput.isSlave &&  storageNodesPubs.nonEmpty)
//              Helpers.activeReplicationV5(
//                payload =  payload.copy(
//                    source = source.toString,
//                    url    =  url
//                ),
//                storageNodes =storageNodesPubs
//              )
//            else if(replicationStrategy == ReplicationStrategies.PASSIVE && storageNodesPubs.nonEmpty)
//              Helpers.passiveReplicationV5(payload,metadata = uploadFileOutput.metadata)
//            else  Helpers.replicationCompleted(taskId,chunkId,location)
//            else  for {
////              SEND REPLICATION COMPLETED
//              _ <- ctx.logger.debug(s"REPLICATION_COMPLETED_SENT $lbExchange $lbRk")
//              properties = AmqpProperties(headers = Map("commandId" -> StringVal("REPLICATION_COMPLETED") ))
//
//              msgPayload = PAYLOADS.ReplicationCompleted(
//                replicationTaskId = payload.id,
//                storageNodeId     = nodeId,
//                chunkId           = uploadFileOutput.metadata.filename.value,
//                timestamp         = timestamp,
//                location          = Location(url=url,source =source.toString)
//              ).asJson.noSpaces
//              msg      = AmqpMessage[String](payload = msgPayload,properties = properties)
//              _ <- lbPub.publish(msg)
//            } yield ()


          } yield ()
        }
      }
    }
//  ____________________________________________________
    processMessageV2[IO,Payloads.UploadV5,NodeContextV5](
      successCallback = successCb,
      errorCallback =  e=>ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
    )
  }

  def activeRep()(implicit ctx:NodeContextV5,envelope: AmqpEnvelope[String],acker:Acker) = {

    def successCb(payload:Payloads.ActiveRep) = {
      type E                       = NodeError
      val maybeCurrentState        = EitherT.liftF[IO,E,NodeStateV5](ctx.state.get)
      implicit val rabbitMQContext = ctx.rabbitMQContext
      implicit val logger          = ctx.logger
      val L                        = Logger.eitherTLogger[IO,E]
      val app = for {
        timestamp    <- liftFF[Long](IO.realTime.map(_.toMillis))
        currentState <- maybeCurrentState
        sourceFile   = new File(payload.source)
        sourcePath   = sourceFile.toPath
        exists       = sourceFile.exists()
        _            <- if(exists) L.debug("EXISTS")
        else L.error("DOES NOT EXIST")
      } yield ()
      app.value.stopwatch.flatMap{ res =>
        res.result match {
          case Left(e) =>  acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
          case Right(value) => for {
            _        <- acker.ack(envelope.deliveryTag)
            duration = res.duration.toMillis
            _        <- ctx.logger.info(s"ACTIVE_REPLICATION $duration")
          } yield ()
        }
      }
    }
    processMessageV2[IO,Payloads.ActiveRep,NodeContextV5](
      successCallback = successCb,
      errorCallback =  e=>ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
    )
  }

//  DEPRECATED
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
