package mx.cinvestav.handlers
import cats.implicits._
import cats.effect._
import io.circe.DecodingFailure
import io.circe.generic.auto._
import io.circe.syntax._
import io.circe._
import mx.cinvestav.Helpers
import mx.cinvestav.Main.NodeContext
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.domain.{CommandId, NodeState}
import mx.cinvestav.domain.Payloads.AddReplicas
import mx.cinvestav.utils.{Command, RabbitMQUtils}
import org.typelevel.log4cats.Logger

class AddReplicasHandler(command: Command[Json], state:Ref[IO,NodeState])(implicit ctx:NodeContext[IO])  extends CommandHandler [IO,AddReplicas]{
  override def handleLeft(df: DecodingFailure): IO[Unit] = ctx.logger.error(df.getMessage())

  override def handleRight(payload: AddReplicas): IO[Unit] = for {
    _ <- ctx.logger.debug(CommandId.ADD_REPLICAS+s" ${payload.id} ${payload.fileId}")
    _ <- ctx.helpers.addReplicas(payload.fileId,payload.replica,state)
  } yield ()

  override def handle(): IO[Unit] = handler(command.payload.as[AddReplicas])
}
object AddReplicasHandler {
  def apply(command:Command[Json],state:Ref[IO,NodeState])(implicit ctx:NodeContext[IO]): IO[Unit] =
    new AddReplicasHandler(command,state).handle()
}
