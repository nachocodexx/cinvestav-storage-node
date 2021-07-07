package mx.cinvestav
import cats.effect.Ref
import cats.implicits._
import dev.profunktor.fs2rabbit.model.ExchangeType
import fs2.concurrent.SignallingRef
import mx.cinvestav.commons.balancer
import mx.cinvestav.domain.{CommandId, NodeState, Payloads}
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.domain.Constants.ReplicationStrategies
import mx.cinvestav.domain.Payloads.{ActiveReplication, ActiveReplicationDone, PassiveReplication}
import mx.cinvestav.handlers.{ActiveReplicationDoneHandler, ActiveReplicationHandler, AddReplicasHandler, DownloadFileHandler, PassiveReplicationHandler, UploadHandler}
import mx.cinvestav.utils.{Command, RabbitMQUtils}
import mx.cinvestav.utils.RabbitMQUtils.dynamicRabbitMQConfig
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.io.File
import java.net.InetAddress
//
import cats.effect.{ExitCode, IO, IOApp}
//
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
//
import pureconfig._
import pureconfig.generic.auto._
//
import scala.concurrent.duration._
import scala.language.postfixOps
import mx.cinvestav.commons.status
import mx.cinvestav.commons.payloads
import mx.cinvestav.commons.commands.Identifiers
// Circe
import io.circe._,io.circe.generic.auto._,io.circe.parser._,io.circe.syntax._

object Main extends IOApp{
  implicit val config: DefaultConfig                       = ConfigSource.default.loadOrThrow[DefaultConfig]
  val rabbitMQConfig: Fs2RabbitConfig                      = dynamicRabbitMQConfig(config.rabbitmq)
  case class NodeContext[F[_]](config: DefaultConfig,logger: Logger[F],helpers: Helpers,utils: RabbitMQUtils[F],state:Ref[IO,NodeState])
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  def program(queueName:String=config.nodeId,state:Ref[IO,NodeState])(implicit utils: RabbitMQUtils[IO],H:Helpers,ctx:NodeContext[IO]):IO[Unit] =
    for {
    _ <- utils.consumeJson(queueName)
      .evalMap {
        command =>
          command.commandId match {
            case Identifiers.NEW_COORDINATOR         => CommandHandlers.newCoordinator(command,state)
            case Identifiers.NEW_COORDINATOR_V2      => CommandHandlers.newCoordinatorV2(command,state)
            case CommandId.DOWNLOAD_FILE             => DownloadFileHandler(command)
            case CommandId.UPLOAD_FILE               => UploadHandler(command,state)
            case CommandId.PASSIVE_REPLICATION       => PassiveReplicationHandler(command,state)
//
            case CommandId.ACTIVE_REPLICATION        => ActiveReplicationHandler(command,state)
            case CommandId.ACTIVE_REPLICATION_DONE   => ActiveReplicationDoneHandler(command,state)
//
            case CommandId.ADD_REPLICAS               => AddReplicasHandler(command,state)
            case Identifiers.START_HEARTBEAT         => CommandHandlers.startHeartbeat(command,state)
            case Identifiers.STOP_HEARTBEAT          => CommandHandlers.stopHeartbeat(command,state)
            case CommandId.RESET                     => CommandHandlers.reset(command,state)
            case _ => state.get.map(_.metadata).flatMap(IO.println)
//              IO.println("UNKNOWN_COMMAND")
          }
      }
      .compile.drain
  } yield ()


  def initQueues(mainQueueName:String)(implicit utils: RabbitMQUtils[IO]): IO[Unit] = for {
    _               <- utils.createQueue(
      queueName    = mainQueueName,
      exchangeName =  config.poolId,
      exchangeType = ExchangeType.Topic,
      routingKey   =  s"${config.poolId}.${config.nodeId}.default"
    )
    _               <- utils.bindQueue(
      queueName    = mainQueueName,
      exchangeName = config.poolId,
      routingKey   = s"${config.poolId}.#.config")
  } yield ()


  override def run(args: List[String]): IO[ExitCode] = {
    RabbitMQUtils.init[IO](rabbitMQConfig){ implicit utils=>
        for {
          _               <- Logger[IO].info(config.toString)
          _               <- Logger[IO].info(s"STORAGE NODE[${config.nodeId}] is up and running 🚀")
//
          heartbeatSignal <- SignallingRef[IO,Boolean](false)
//
          rootFile        = new File("/")
          _initState      = NodeState(
            status              = status.Up,
            heartbeatSignal     = heartbeatSignal,
            loadBalancer        = balancer.LoadBalancer(config.loadBalancer),
            replicationFactor   = config.replicationFactor,
            storagesNodes       = config.storageNodes,
            ip                  = InetAddress.getLocalHost.getHostAddress,
            availableResources  = config.storageNodes.length+1,
            replicationStrategy = config.replicationStrategy,
            freeStorageSpace    = rootFile.getFreeSpace,
            usedStorageSpace    = rootFile.getTotalSpace - rootFile.getFreeSpace,
            chordRoutingKey          = s"${config.poolId}.global.chord"
          )
          state           <- IO.ref(_initState)
          //        MAIN PROGRAM
          mainQueueName   <- IO.pure(s"${config.poolId}-${config.nodeId}")
          _               <- initQueues(mainQueueName)
          _               <- Logger[IO].debug(s"[START STORAGE NODE] ${_initState}")
          helpers         = Helpers()
          ctx             = NodeContext[IO](config,logger = unsafeLogger,helpers=helpers,utils = utils,state=state)
          _               <- program(mainQueueName,state)(utils,helpers,ctx)
        } yield ()
      }
    }.as(ExitCode.Success)
}
