package mx.cinvestav.handlers
import cats.implicits._
import cats.effect._
import io.circe.{DecodingFailure, Json}
import io.circe.syntax._
import io.circe.generic.auto._
import mx.cinvestav.Helpers
import mx.cinvestav.commons.commands.CommandData
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.domain.Errors.FileNotFound
import mx.cinvestav.domain.{CommandId, NodeState, Payloads, Replica}
import mx.cinvestav.utils.{Command, RabbitMQUtils}
import org.typelevel.log4cats.Logger

class ActiveReplicationDoneHandler(command: Command[Json],state:Ref[IO,NodeState])(implicit config:DefaultConfig, logger: Logger[IO],H:Helpers,utils:RabbitMQUtils[IO]) extends CommandHandler[IO,Payloads.ActiveReplicationDone] {
  override def handleLeft(df: DecodingFailure): IO[Unit] = Logger[IO].error(df.getMessage())

  override def handleRight(payload: Payloads.ActiveReplicationDone): IO[Unit] = for {
    _                 <- Logger[IO].debug(s"ACTIVE_REPLICATION_DONE ${payload.id} ${payload.replica.nodeId}")
    _                 <- H.addReplicas(payload.fileId,payload.replica::Nil,state)
    //    Propagate metadata
    currentState      <- state.updateAndGet(s=>s.copy(
      activeReplicationCompletion = s.activeReplicationCompletion.updatedWith(payload.fileId)(_.map(_-1)))
    )
    nodeIds           <- currentState.storagesNodes.filter(_!= payload.replica.nodeId).pure[IO]
    routingKey        = (nodeId:String) => s"${config.poolId}.$nodeId.default"
    publishers        <- nodeIds.traverse(nodeId => utils.fromNodeIdToPublisher(nodeId,config.poolId,routingKey(nodeId)))
    _payload          = Payloads.AddReplicas(
      id           = payload.id,
      fileId       = payload.fileId,
      replica      =  payload.replica::Nil,
      experimentId = payload.experimentId
    ).asJson
    cmd               = CommandData[Json](CommandId.ADD_REPLICAS,_payload).asJson.noSpaces
    _                 <- publishers.traverse(_.publish(cmd))
//    Send to chord
    replicationIsFinish = currentState.activeReplicationCompletion.get(payload.fileId)
    _ <- replicationIsFinish match {
      case Some(value) => if(value==0) IO.unit else IO.unit
      case None => IO.unit
    }
  } yield ()

  override def handle(): IO[Unit] = handler(command.payload.as[Payloads.ActiveReplicationDone])
}
object ActiveReplicationDoneHandler {
  def apply(command:Command[Json],state:Ref[IO,NodeState])(implicit logger: Logger[IO],H:Helpers,config: DefaultConfig,utils: RabbitMQUtils[IO]): IO[Unit] =
    new ActiveReplicationDoneHandler(command, state).handle()
}
