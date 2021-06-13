package mx.cinvestav
import cats.effect.Ref
import cats.implicits._
import dev.profunktor.fs2rabbit.model.ExchangeType
import fs2.concurrent.SignallingRef
import mx.cinvestav.domain.{CommandId, NodeState, Payloads}
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.utils.{Command, RabbitMQUtils}
import mx.cinvestav.utils.RabbitMQUtils.dynamicRabbitMQConfig
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
import mx.cinvestav.commons.commands.CommandData
// Circe
import io.circe._,io.circe.generic.auto._,io.circe.parser._,io.circe.syntax._

object Main extends IOApp{
//  type NodeState =Map[String,Any]
  implicit val config: DefaultConfig  = ConfigSource.default.loadOrThrow[DefaultConfig]
  val rabbitMQConfig: Fs2RabbitConfig = dynamicRabbitMQConfig(config.rabbitmq)


//  def initStateIO(): IO[Map[String, Any]] = IO.pure{
//    Map(
//      "id"            -> config.nodeId,
//      "status"        -> status.Up.toString(),
//      "loadBalancer"  -> config.loadBalancer,
//      "rf"            -> config.replicationFactor,
//      "poolId"        -> config.poolId,
////      "priority"      -> config.priority,
//      "storageNodes"  -> config.storageNodes
//    )
//  }


  def program(queueName:String=config.nodeId,state:Ref[IO,NodeState])(implicit utils: RabbitMQUtils[IO]):IO[Unit] =
    for {
    _ <- utils.consumeJson(queueName)
      .evalMap {
        command =>
          command.commandId match {
            case CommandId.DOWNLOAD_FILE =>
              CommandHandler.downloadFile(command)
            case CommandId.UPDATE_REPLICATION_FACTOR =>
              CommandHandler.updateReplicationFactor(command, state)
            case CommandId.START_HEARTBEAT =>
              CommandHandler.startHeartbeat(command,state)
            case CommandId.STOP_HEARTBEAT =>
              CommandHandler.stopHeartbeat(command,state)
            case _ => IO.println("UNKNOWN COMMAND")
          }
      }
      .compile.drain
  } yield ()




  override def run(args: List[String]): IO[ExitCode] = {
    RabbitMQUtils.init[IO](rabbitMQConfig){ implicit utils=>
        for {
          heartbeatSignal <- SignallingRef[IO,Boolean](false)
          _initState <- NodeState(
            status = status.Up,
            heartbeatSignal = heartbeatSignal,
            loadBalancer =  config.loadBalancer,
            replicationFactor =  config.replicationFactor
          ).pure[IO]
          state              <- IO.ref(_initState)
          //          HEARTBEAT
          //        MAIN PROGRAM
//          initState          <- initStateIO()
          queueName          <- IO.pure(s"${config.poolId}-${config.nodeId}")
          _                  <- utils.createQueue(
                queueName    = queueName,
                exchangeName =  config.poolId,
                exchangeType = ExchangeType.Topic,
                routingKey   =  s"${config.poolId}.${config.nodeId}.default"
          )
          _                  <- utils.bindQueue(queueName = queueName,exchangeName = config.poolId,"#.config")
//          state              <- IO.ref(initState)
          _                  <- program(queueName,state)
        } yield ()
      }
    }.as(ExitCode.Success)
}
