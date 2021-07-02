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


  def handleBeforeSaveFile(transferred:Long,payload:Payloads.PassiveReplication) =
      for {
      timestamp           <- IO.realTime.map(_.toSeconds)
      ip                  <- state.get.map(_.ip)
      replica             = Replica(ctx.config.nodeId,primary = false,0,timestamp)
      ext                 = CompressionUtils.getExtensionByCompressionAlgorithm(payload.metadata.compressionAlgorithm)
      fileMetadata        = payload.metadata.copy(replicas = payload.metadata.replicas :+ replica)
      replicaCounter      = fileMetadata.replicas.length
      totalOfReplicas     = payload.replicationFactor +1
      continueReplication = replicaCounter < totalOfReplicas
//      addReplicaPayload   = Payloads.AddReplica(payload.id,payload.fileId,replica).asJson
//      addReplicateCmd     = CommandData[Json](CommandId.ADD_REPLICA,addReplicaPayload).asJson.noSpaces
      newPayload          = payload.copy(metadata = fileMetadata,url = s"http://$ip/${payload.fileId}.$ext",lastNodeId = ctx.config.nodeId)
      replicasNodes       = fileMetadata.replicas.map(_.nodeId)
      _                  <- state.updateAndGet(s=>s.copy(metadata = s.metadata + (payload.fileId -> fileMetadata) ))
      lastNodePub        <- ctx.utils.fromNodeIdToPublisher(payload.lastNodeId,ctx.config.poolId,s"${ctx.config.poolId}.${payload.lastNodeId}.default")
//      _                  <- lastNodePub.publish(addReplicateCmd)
      _                 <- if(continueReplication) ctx.helpers._passiveReplication(state,replicasNodes,newPayload) else ctx.logger.debug("NO CONTINUE")

      //    _                  <- ctx.logger.debug("REPLICATION COUNTER: "+replicaCounter)
      //    _                  <- ctx.logger.debug("REPLICATION FACTOR: "+totalOfReplicas)
    } yield ()

  override def handleRight(payload: Payloads.PassiveReplication): IO[Unit] = for {
    _          <- ctx.logger.debug(CommandId.PASSIVE_REPLICATION+ s" ${payload.id} ${payload.fileId} ${payload.userId} ${payload.lastNodeId}")
    url        = new URL(payload.url)
//    extension  = url.getPath.split("\\.").lastOption
    outputPath = s"${ctx.config.storagePath}${url.getPath}"
//    _ <- ctx.logger.debug(extension.toString)
    _          <- ctx.helpers.downloadFileFormURL(payload.fileId,outputPath,url).value.flatMap {
      case Left(value) => ctx.logger.error(value.message)
      case Right(value) => handleBeforeSaveFile(value,payload)
    }
  } yield ()



  override def handle(): IO[Unit] = handler(command.payload.as[Payloads.PassiveReplication])
}

object PassiveReplicationHandler {
  def apply(command: Command[Json],state:Ref[IO,NodeState])(implicit ctx:NodeContext[IO]): IO[Unit] =
    new PassiveReplicationHandler(command,state).handle()
}
