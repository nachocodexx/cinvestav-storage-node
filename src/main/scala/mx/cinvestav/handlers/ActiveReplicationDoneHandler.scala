package mx.cinvestav.handlers
import cats.implicits._
import cats.effect._
import io.circe.{DecodingFailure, Json}
import io.circe.syntax._
import io.circe.generic.auto._
import mx.cinvestav.Helpers
import mx.cinvestav.Main.NodeContext
import mx.cinvestav.commons.commands.{CommandData, Identifiers}
import mx.cinvestav.commons.payloads
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.domain.Errors.FileNotFound
import mx.cinvestav.domain.{CommandId, NodeState, Payloads}
import mx.cinvestav.commons.storage.{Replica,FileMetadata}
import mx.cinvestav.utils.{Command, RabbitMQUtils}
import org.typelevel.log4cats.Logger

class ActiveReplicationDoneHandler(command: Command[Json],state:Ref[IO,NodeState])(implicit ctx:NodeContext[IO]) extends CommandHandler[IO,Payloads.ActiveReplicationDone] {
  def sendMetadataToChord(payload:Payloads.ActiveReplicationDone,metadata:FileMetadata)(implicit ctx:NodeContext[IO]):IO[Unit ] = for {
    _             <- IO.unit
    addKeyPayload = payloads.AddKey(
      id=payload.id,
      key=payload.fileId,
      value = metadata.asJson.noSpaces,
      experimentId = payload.experimentId
    )
    addKeyCmd     = CommandData[Json](Identifiers.ADD_KEY,addKeyPayload.asJson)
    _             <- ctx.state.update(s=>s.copy(activeReplicationCompletion = s.activeReplicationCompletion.removed(payload.fileId)))
    _             <- ctx.helpers.sendMetadataToChord(addKeyCmd)
  } yield ()
  override def handleLeft(df: DecodingFailure): IO[Unit] = ctx.logger.error(df.getMessage())

  override def handleRight(payload: Payloads.ActiveReplicationDone): IO[Unit] = for {
    _                 <- ctx.logger.debug(s"ACTIVE_REPLICATION_DONE ${payload.id} ${payload.replica.nodeId} ${payload.experimentId}")
    _                 <- ctx.helpers.addReplicas(payload.fileId,payload.replica::Nil,state)
    //    Propagate metadata
    currentState      <- ctx.state.updateAndGet(s=>s.copy(
      activeReplicationCompletion = s.activeReplicationCompletion.updatedWith(payload.fileId)(_.map(_-1)))
    )
    nodeIds           <- currentState.storagesNodes.filter(_!= payload.replica.nodeId).pure[IO]
    routingKey        = (nodeId:String) => s"${ctx.config.poolId}.$nodeId.default"
    publishers        <- nodeIds.traverse(nodeId => ctx.utils.fromNodeIdToPublisher(nodeId,ctx.config.poolId,routingKey(nodeId)))
    _payload          = Payloads.AddReplicas(
      id           = payload.id,
      fileId       = payload.fileId,
      replica      =  payload.replica::Nil,
      experimentId = payload.experimentId
    ).asJson
    maybeMetadata      = currentState.metadata.get(payload.fileId)
    cmd               = CommandData[Json](CommandId.ADD_REPLICAS,_payload).asJson.noSpaces
    _                 <- publishers.traverse(_.publish(cmd))
    //    Send to chord
    replicationIsFinish = currentState.activeReplicationCompletion.get(payload.fileId)
     _ <- replicationIsFinish.mproduct(_ => maybeMetadata) match {
       case Some((value,metadata)) =>  if(value ==0) sendMetadataToChord(payload,metadata) else IO.unit
       case None => ctx.logger.error("SOMETHING WENT WRONG :c")
     }
//    _ <- replicationIsFinish match {
//      case Some(value) =>
//        if(value==0) sendMetadataToChord(payload,)
//        else IO.unit
//      case None => IO.unit
//    }
  } yield ()

  override def handle(): IO[Unit] = handler(command.payload.as[Payloads.ActiveReplicationDone])
}
object ActiveReplicationDoneHandler {
  def apply(command:Command[Json],state:Ref[IO,NodeState])(implicit ctx:NodeContext[IO]): IO[Unit] =
    new ActiveReplicationDoneHandler(command, state).handle()
}
