package mx.cinvestav.handlers
import cats.implicits._
import cats.effect._
import io.circe.{DecodingFailure, Json}
import io.circe.generic.auto._
import io.circe.syntax._
import mx.cinvestav.Helpers
import mx.cinvestav.commons.commands.CommandData
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.domain.Payloads.ActiveReplicationDone
import mx.cinvestav.domain.{CommandId, NodeState, Payloads, Replica}
import mx.cinvestav.utils.{Command, RabbitMQUtils}
import org.typelevel.log4cats.Logger

import java.net.URL

class ActiveReplicationHandler(command: Command[Json],state:Ref[IO,NodeState])(implicit config:DefaultConfig, logger: Logger[IO],H:Helpers,utils:RabbitMQUtils[IO]) extends CommandHandler[IO,Payloads
.ActiveReplication]{
  override def handleLeft(df: DecodingFailure): IO[Unit] = Logger[IO].error(df.message)

  def log(payload:Payloads.ActiveReplication):IO[Unit] =
    Logger[IO].debug(CommandId.ACTIVE_REPLICATION+s" ${payload.id} ${payload.fileId}")
  override def handleRight(payload: Payloads.ActiveReplication): IO[Unit] = {
    for {
      _        <- log(payload)
      url      = new URL(payload.url)
      filePath = s"${config.storagePath}${url.getPath}"
      _         <- H.downloadFileFormURL(payload.fileId,filePath = filePath,url).value.flatMap {
        case Left(value) => Logger[IO].error(value.message)
        case Right(value) =>for {
          timestamp <- IO.realTime.map(_.toSeconds)
//          Create replica
          replica  = Replica(config.nodeId,primary = false,0,timestamp)
//        Update metadata
          fileMetadata = payload.metadata.copy(replicas = payload.metadata.replicas :+ replica)
          _         <- state.update(s=>s.copy(metadata =s.metadata + (payload.fileId->fileMetadata ) ))
//       Send replicas to balanced storage nodes
          publisher <- utils.fromNodeIdToPublisher(payload.leaderNodeId,config.poolId,s"${config.poolId}.${payload.leaderNodeId}.default")
          activeReplicationDonePayload  = ActiveReplicationDone(id= payload.id,replica= replica,fileId = payload.fileId,transferred = value)
          cmd      = CommandData[Json](CommandId.ACTIVE_REPLICATION_DONE,activeReplicationDonePayload.asJson).asJson.noSpaces
          _         <- publisher.publish(cmd)

        } yield ()
      }

    } yield ()
  }

  override def handle(): IO[Unit] = handler(command.payload.as[Payloads.ActiveReplication])
}
object ActiveReplicationHandler {
  def apply(command:Command[Json],state:Ref[IO,NodeState])(implicit logger: Logger[IO],H:Helpers,config: DefaultConfig,utils: RabbitMQUtils[IO]): IO[Unit] = new ActiveReplicationHandler(command,state)
    .handle()
}
