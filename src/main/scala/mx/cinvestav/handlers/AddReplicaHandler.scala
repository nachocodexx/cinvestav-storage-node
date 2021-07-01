package mx.cinvestav.handlers
import cats.implicits._
import cats.effect._
import io.circe.DecodingFailure
import io.circe.generic.auto._,io.circe.syntax._,io.circe._
import mx.cinvestav.Helpers
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.domain.NodeState
import mx.cinvestav.domain.Payloads.AddReplica
import mx.cinvestav.utils.{Command, RabbitMQUtils}
import org.typelevel.log4cats.Logger

class AddReplicaHandler(command: Command[Json],state:Ref[IO,NodeState])(implicit config:DefaultConfig, logger: Logger[IO],H:Helpers,utils:RabbitMQUtils[IO])  extends CommandHandler [IO,AddReplica]{
  override def handleLeft(df: DecodingFailure): IO[Unit] = Logger[IO].error(df.getMessage())

  override def handleRight(payload: AddReplica): IO[Unit] = for {
    _ <- Logger[IO].debug(s"ADD_REPLICA ${payload.id} ${payload.fileId}")
    _ <- H.addReplica(payload.fileId,payload.replica,state)
  } yield ()

  override def handle(): IO[Unit] = handler(command.payload.as[AddReplica])
}
object AddReplicaHandler {
  def apply(command:Command[Json],state:Ref[IO,NodeState])(implicit logger: Logger[IO],H:Helpers,config: DefaultConfig,utils: RabbitMQUtils[IO]): IO[Unit] =
    new AddReplicaHandler(command,state).handle()
}
