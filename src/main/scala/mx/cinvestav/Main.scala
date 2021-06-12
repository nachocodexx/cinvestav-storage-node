package mx.cinvestav
import cats.effect.Ref
import cats.implicits._
import dev.profunktor.fs2rabbit.model.ExchangeType
import mx.cinvestav.domain.{CommandId, Payloads}
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
  type NodeState =Map[String,Any]
  implicit val config: DefaultConfig  = ConfigSource.default.loadOrThrow[DefaultConfig]
  val rabbitMQConfig: Fs2RabbitConfig = dynamicRabbitMQConfig(config.rabbitmq)
  case class CO(commandId:String,payload:Json)
  import mx.cinvestav.Parsers._


  def initStateIO(): IO[Map[String, Any]] = IO.pure{
    Map(
      "id"            -> config.nodeId,
      "status"        -> status.Up.toString(),
      "loadBalancer"  -> config.loadBalancer,
      "rf"            -> config.replicationFactor,
      "poolId"        -> config.poolId,
      "priority"      -> config.priority,
      "storageNodes"  -> config.storageNodes
    )
  }


  def program(queueName:String=config.nodeId,state:Ref[IO,NodeState])(implicit RMQU: RabbitMQUtils[IO]):IO[Unit] = for {
    _ <- RMQU.consumeJson(queueName)
      .evalMap {
        command =>
          command.commandId match {
            case CommandId.DOWNLOAD_FILE =>
              CommandProcessor.downloadFile(command)
            case CommandId.UPDATE_REPLICATION_FACTOR =>
              CommandProcessor.updateReplicationFactor(command, state)
          }
      }
      .compile.drain
  } yield ()

  def heartbeat(value:Int,publisher:String=>IO[Unit])(implicit utils:RabbitMQUtils[IO]):IO[Unit] = for {
    _       <- IO.println("HEART BEAT.... <3")
    beat    <- payloads.HeartbeatPayload(value=value).pure[IO].map(_.asJson)
    _       <- IO.println(beat)
    command <- CommandData[Json](CommandId.HEARTBEAT,payload  = beat).pure[IO]
    _       <- publisher(command.asJson.noSpaces)
  } yield ()

  override def run(args: List[String]): IO[ExitCode] = {
    RabbitMQUtils.init[IO](rabbitMQConfig){ implicit utils=>
        for {
//          HEARTBEAT
//          heartbeatQueue <- s"${config.nodeId}-heartbeat".pure[IO]
//          heartbeatRk       <- s"${config.poolId}.${config.nodeId}.heartbeat".pure[IO]
          heartbeatQueue <- s"${config.poolId}-heartbeat".pure[IO]
          heartbeatRk       <- s"${config.poolId}.heartbeat".pure[IO]
          _                 <- utils.createQueue(heartbeatQueue,config.poolId,ExchangeType.Topic,heartbeatRk)
//        HEARTBEAT PUBLISHER
          heartbeatPublisher <- utils.createPublisher(config.poolId,heartbeatRk)
          _                  <- utils.publishEveryEffect(heartbeat(_,heartbeatPublisher),1 seconds)
//        MAIN PROGRAM
          initState          <- initStateIO()
          queueName          <- IO.pure(s"${config.poolId}-${config.nodeId}")
          _                  <- utils.createQueue(
                queueName    = queueName,
                exchangeName =  config.poolId,
                exchangeType = ExchangeType.Topic,
                routingKey   =  s"${config.poolId}.${config.nodeId}.default"
          )
          _                  <- utils.bindQueue(queueName = queueName,exchangeName = config.poolId,"#.config")
          state              <- IO.ref(initState)
          _                  <- program(queueName,state)
        } yield ()
      }
    }.as(ExitCode.Success)
}
