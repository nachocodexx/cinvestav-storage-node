package mx.cinvestav.handlers
import cats.implicits._
import cats.effect._
import io.circe.{DecodingFailure, Json}
import io.circe.syntax._
import io.circe.generic.auto._
import mx.cinvestav.Helpers
import mx.cinvestav.Main.NodeContext
import mx.cinvestav.commons.commands.CommandData
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.domain.Constants.CompressionUtils
import mx.cinvestav.domain.{CommandId, NodeState, Payloads, Replica}
import mx.cinvestav.utils.{Command, RabbitMQUtils}
import org.typelevel.log4cats.Logger

import java.net.URL


class PassiveReplicationHandler(command: Command[Json],state:Ref[IO,NodeState])(implicit ctx:NodeContext[IO]) extends CommandHandler [IO,Payloads.PassiveReplication]{
  override def handleLeft(df: DecodingFailure): IO[Unit] = ctx.logger.error(df.getMessage())


  def handleAfterSaveFile(transferred:Long, payload:Payloads.PassiveReplication): IO[Unit] =
      for {
//        _
      timestamp           <- IO.realTime.map(_.toSeconds)
      replica             = Replica(ctx.config.nodeId,primary = false,0,timestamp)
      ext                 = CompressionUtils.getExtensionByCompressionAlgorithm(payload.metadata.compressionAlgorithm)
      fileMetadata        = payload.metadata.copy(replicas = payload.metadata.replicas :+ replica)
      currentState        <- state.updateAndGet(s=>s.copy(metadata = s.metadata + (payload.fileId -> fileMetadata) ))
      ip                  = currentState.ip
      replicaCounter      = fileMetadata.replicas.length
      totalOfReplicas     = payload.replicationFactor +1
      continueReplication = replicaCounter < totalOfReplicas
      newPayload          = payload.copy(metadata = fileMetadata,url = s"http://$ip/${payload.fileId}.$ext",lastNodeId = ctx.config.nodeId)
      replicasNodes       = fileMetadata.replicas.map(_.nodeId)
      _                 <- if(continueReplication) ctx.helpers._passiveReplication(state,replicasNodes,newPayload)
      else for {
//        _ <- IO.unit
        _ <- ctx.logger.debug(s"PROPAGATE_METADATA ${payload.id} ${payload.metadata.replicas.map(_.nodeId).mkString(",")}")
        exchangeName      = ctx.config.poolId
        routingKey        = (nId:String) => s"$exchangeName.$nId.default"
        addReplicaPayload =Payloads.AddReplicas(
          id      = payload.id,
          fileId  = payload.fileId,
          replica =  fileMetadata.replicas
        ).asJson
        completedReplicasIds = payload.metadata.replicas.map(_.nodeId)
        addReplicasCmd       = CommandData[Json](CommandId.ADD_REPLICAS,addReplicaPayload).asJson.noSpaces
        publishers        <- completedReplicasIds.traverse(nId => ctx.utils.fromNodeIdToPublisher(nId,exchangeName,routingKey(nId)))
        _                 <- publishers.traverse(publisher => publisher.publish(addReplicasCmd))
        _                 <- state.updateAndGet(s=>s.copy(metadata = s.metadata + (payload.fileId -> fileMetadata) ))
      } yield ()
    } yield ()

  override def handleRight(payload: Payloads.PassiveReplication): IO[Unit] = for {
    _          <- ctx.logger.debug(CommandId.PASSIVE_REPLICATION+ s" ${payload.id} ${payload.fileId} ${payload.userId} ${payload.lastNodeId}")
    url        = new URL(payload.url)
    outputPath = s"${ctx.config.storagePath}${url.getPath}"
    _          <- ctx.helpers.downloadFileFormURL(payload.fileId,outputPath,url).value.flatMap {
      case Left(value) => ctx.logger.error(value.message)
      case Right(value) => handleAfterSaveFile(value,payload)
    }
  } yield ()



  override def handle(): IO[Unit] = handler(command.payload.as[Payloads.PassiveReplication])
}

object PassiveReplicationHandler {
  def apply(command: Command[Json],state:Ref[IO,NodeState])(implicit ctx:NodeContext[IO]): IO[Unit] =
    new PassiveReplicationHandler(command,state).handle()
}
