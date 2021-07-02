package mx.cinvestav.handlers
import cats.data.EitherT
import cats.implicits._
import cats.effect._
import io.circe.Decoder.Result
import io.circe.{DecodingFailure, Json}
import mx.cinvestav.Main.{NodeContext, unsafeLogger}
//import io.circe.generic.aut
import io.circe.generic.auto._
import io.circe.syntax._
import mx.cinvestav.commons.commands.CommandData
import mx.cinvestav.config.RabbitMQConfig
import mx.cinvestav.domain.Constants.ReplicationStrategies
import mx.cinvestav.utils.RabbitMQUtils
//import mx.cinvestav.CommandHandlers.saveAndCompress
import mx.cinvestav.Helpers
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.domain.Errors.{Failure, RFGreaterThanAR}
import mx.cinvestav.domain.{CommandId, Errors, FileMetadata, NodeState, Payloads}
import mx.cinvestav.utils.Command
import org.typelevel.log4cats.Logger

class UploadHandler(command: Command[Json],state:Ref[IO,NodeState])(implicit ctx:NodeContext[IO]) extends CommandHandler[IO, Payloads.UploadFile]{

  def logE(payload: Payloads.UploadFile): EitherT[IO, Failure, Unit] =
    Logger.eitherTLogger[IO,Failure].debug(
      CommandId.UPLOAD_FILE+s" ${payload.id} ${payload.fileId} ${payload.userId} ${payload.url} " +s"${payload.replicationFactor} ${payload.experimentId}"
    )

  def handleSaveAndCompressError(failure: Failure): IO[Unit] =  failure match {
    case Errors.DuplicatedReplica(fileId,_) =>
      ctx.logger.error(s"DUPLICATED_REPLICA $fileId")
    case Errors.FileNotFound(filename,_) =>
      ctx.logger.error(s"FILE_NOT_FOUND $filename")
    case Errors.CompressionFail(message) =>
      ctx.logger.error(message)
    case RFGreaterThanAR(message) =>
      ctx.logger.error(message)
    case _ =>
      ctx.logger.error("UNKNOWN_ERROR")
  }

  def handleSaveAndCompressSuccess(payload:Payloads.UploadFile,metadata: FileMetadata): IO[Unit] = for {
      currentState <- state.updateAndGet(s=>s.copy(metadata = s.metadata+(payload.fileId->metadata)))
       _           <- if(currentState.replicationStrategy == ReplicationStrategies.PASSIVE)   ctx.helpers.buildPassiveReplication(payload,metadata,state)
       else ctx.helpers.activeReplication(payload,metadata,currentState)
    } yield ( )

  override def handleLeft(df: DecodingFailure): IO[Unit] = ctx.logger.error(df.getMessage())
  override def handleRight(payload: Payloads.UploadFile): IO[Unit] = {
    val maybeSaveAndCompress:EitherT[IO,Failure,FileMetadata] = for {
      _                <- logE(payload)
      currentState     <- EitherT(state.get.map(_.asRight[Failure]))

      metadata = for {
        maybeMeta        <- EitherT.fromEither[IO](currentState.metadata.get(payload.fileId).asRight[Failure])
        metadata         <- ctx.helpers.saveAndCompress(payload,maybeMeta)
      } yield metadata

      m <- if(currentState.availableResources <= payload.replicationFactor)
        EitherT.fromEither[IO](Either.left[Failure,FileMetadata](RFGreaterThanAR()))
      else metadata
    } yield m

    maybeSaveAndCompress.value.flatMap {
      case Left(e) => handleSaveAndCompressError(e)
      case Right(metadata) => handleSaveAndCompressSuccess(payload,metadata)
    }

  }

  override def handle(): IO[Unit] =
    handler(command.payload.as[Payloads.UploadFile])
}

object UploadHandler{
  def apply(command:Command[Json],state:Ref[IO,NodeState])(implicit ctx:NodeContext[IO]): IO[Unit] =
    new UploadHandler(command,state).handle()
}
