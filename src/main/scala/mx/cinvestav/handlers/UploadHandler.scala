package mx.cinvestav.handlers
import cats.data.EitherT
import cats.implicits._
import cats.effect._
import io.circe.{DecodingFailure, Json}
import mx.cinvestav.Main.{NodeContext, unsafeLogger}
import io.circe.generic.auto._
import mx.cinvestav.domain.Constants.ReplicationStrategies
import mx.cinvestav.domain.Errors.{Failure, RFGreaterThanAR}
import mx.cinvestav.domain.{CommandId, Errors, NodeState, Payloads}
import mx.cinvestav.commons.storage.FileMetadata
import mx.cinvestav.utils.Command
import org.typelevel.log4cats.Logger
import io.circe.syntax._
import io.circe.generic.auto._

class UploadHandler(command: Command[Json],state:Ref[IO,NodeState])(implicit ctx:NodeContext[IO]) extends CommandHandler[IO, Payloads.UploadFile]{

  def logE(payload: Payloads.UploadFile): EitherT[IO, Failure, Unit] =
    Logger.eitherTLogger[IO,Failure].debug(
      CommandId.UPLOAD_FILE+s" ${payload.id} ${payload.fileId} ${payload.userId} ${payload.url} " +s"${payload.replicationFactor} ${payload.experimentId}"
    )

  def handleSaveAndCompressError(failure: Failure): IO[Unit] =  failure match {
    case Errors.DuplicatedReplica(fileId,_) =>
      ctx.logger.error(s"DUPLICATED_REPLICA $fileId")
    case e@Errors.FileNotFound(filename) =>
      ctx.logger.error(e.message)
    case Errors.CompressionFail(message) =>
      ctx.logger.error(message)
    case RFGreaterThanAR(message) =>
      ctx.logger.error(message)
    case _ =>
      ctx.logger.error("UNKNOWN_ERROR")
  }

  def doPassiveReplication(payload:Payloads.UploadFile,metadata: FileMetadata):IO[Unit] = for {
    _  <- state.updateAndGet(s=>s.copy(metadata = s.metadata+(payload.fileId->metadata.copy(replicas=Nil))))
    _  <- ctx.helpers.buildPassiveReplication(payload,metadata)
  } yield ()

  def doActiveReplication(payload:Payloads.UploadFile,metadata: FileMetadata):IO[Unit] = for {
      currentState <- state.updateAndGet(s=>s.copy(metadata = s.metadata+(payload.fileId->metadata)))
      _ <- ctx.helpers.activeReplication(payload,metadata)
    } yield ()

  def handleSaveAndCompressSuccess(payload:Payloads.UploadFile,metadata: FileMetadata): IO[Unit] = for {
    _ <- ctx.logger.debug(CommandId.UPLOAD_FILE+s" ${payload.id} ${payload.fileId} ${payload.userId} ${payload.url} " +s"${payload.replicationFactor} ${payload.experimentId}")
    currentState <- state.get
//    Perform replication using a predefined strategy
       _           <- if(currentState.replicationStrategy == ReplicationStrategies.PASSIVE)
                         doPassiveReplication(payload,metadata)
                     else
                         doActiveReplication(payload,metadata)
//                         SEND TO CHORD

    } yield ( )

  override def handleLeft(df: DecodingFailure): IO[Unit] = ctx.logger.error(df.getMessage())
  override def handleRight(payload: Payloads.UploadFile): IO[Unit] = {
    val maybeSaveAndCompress:EitherT[IO,Failure,FileMetadata] = for {
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
