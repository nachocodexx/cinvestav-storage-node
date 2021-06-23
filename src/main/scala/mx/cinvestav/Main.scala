package mx.cinvestav
import cats.effect.Ref
import cats.implicits._
import dev.profunktor.fs2rabbit.model.ExchangeType
import fs2.concurrent.SignallingRef
import mx.cinvestav.commons.balancer
import mx.cinvestav.domain.{CommandId, NodeState, Payloads}
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.utils.{Command, RabbitMQUtils}
import mx.cinvestav.utils.RabbitMQUtils.dynamicRabbitMQConfig
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger

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
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  def program(queueName:String=config.nodeId,state:Ref[IO,NodeState])(implicit utils: RabbitMQUtils[IO],H:Helpers):IO[Unit] =
    for {
    _ <- utils.consumeJson(queueName)
      .evalMap {
        command =>
          command.commandId match {
            case Identifiers.NEW_COORDINATOR =>
              CommandHandler.newCoordinator(command,state)
            case Identifiers.NEW_COORDINATOR_V2 =>
              CommandHandler.newCoordinatorV2(command,state)
            case CommandId.DOWNLOAD_FILE =>
              CommandHandler.downloadFile(command,state)
            case CommandId.UPLOAD_FILE =>
              CommandHandler.uploadFile(command,state)
            case CommandId.REPLICATION =>
              CommandHandler.replication(command,state)
            case CommandId.UPDATE_REPLICATION_FACTOR =>
              CommandHandler.updateReplicationFactor(command, state)
            case Identifiers.START_HEARTBEAT =>
              CommandHandler.startHeartbeat(command,state)
            case Identifiers.STOP_HEARTBEAT =>
              CommandHandler.stopHeartbeat(command,state)
            case _ => state.get.map(_.metadata).flatMap(IO.println)
//              IO.println("UNKNOWN_COMMAND")
          }
      }
      .compile.drain
  } yield ()




  override def run(args: List[String]): IO[ExitCode] = {
    RabbitMQUtils.init[IO](rabbitMQConfig){ implicit utils=>
        for {
          _               <- Logger[IO].trace(config.toString)
          _               <- Logger[IO].trace(s"STORAGE NODE[${config.nodeId}] is up and running 🚀")
          heartbeatSignal <- SignallingRef[IO,Boolean](false)
          _initState      <- NodeState(
            status             = status.Up,
            heartbeatSignal    = heartbeatSignal,
            loadBalancer       = balancer.LoadBalancer(config.loadBalancer),
            replicationFactor  = config.replicationFactor,
            storagesNodes      = config.storageNodes,
            ip                 = InetAddress.getLocalHost.getHostAddress,
            availableResources = config.storageNodes.length+1
          ).pure[IO]
          state           <- IO.ref(_initState)
          //        MAIN PROGRAM
          mainQueueName   <- IO.pure(s"${config.poolId}-${config.nodeId}")
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
          _                  <- Logger[IO].debug("START")
          helpers            = Helpers()
          _                  <- program(mainQueueName,state)(utils,helpers)
        } yield ()
      }
    }.as(ExitCode.Success)
}
