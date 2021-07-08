package mx.cinvestav.handlers
import cats.implicits._
import cats.effect._
import io.circe.{DecodingFailure, Json}
import mx.cinvestav.Main.NodeContext
import mx.cinvestav.domain.{CommandId, Payloads}
import io.circe.syntax._
import io.circe.generic.auto._
import mx.cinvestav.commons.commands.CommandData
import mx.cinvestav.domain.Constants.CompressionUtils
import mx.cinvestav.utils.Command
import mx.cinvestav.domain.Errors
import mx.cinvestav.domain.Payloads.FileFound

class DownloadFileHandler(command:Command[Json])(implicit ctx:NodeContext[IO]) extends CommandHandler[IO,Payloads.DownloadFile]{
  override def handleLeft(df: DecodingFailure): IO[Unit] = ctx.logger.error(df.getMessage())


  override def handleRight(payload: Payloads.DownloadFile): IO[Unit] = for {
    currentState <- ctx.state.get
    fileMetadata = currentState.metadata.get(payload.fileId)
    _            <- fileMetadata match {
      case Some(value) => for {
        _          <- ctx.logger.debug(s"${CommandId.DOWNLOAD_FILE} ${payload.id} ${payload.fileId}")
        ext        = CompressionUtils.getExtensionByCompressionAlgorithm(value.compressionAlgorithm)
        url        = s"http://${currentState.ip}/${payload.fileId}.$ext"
        cmdPayload = FileFound(
          id = payload.id,
          fileId= payload.fileId,
          url = url,
          compressionAlgorithm = value.compressionAlgorithm
        ).asJson
        cmd        = CommandData[Json](CommandId.FILE_FOUND,cmdPayload)
        _          <- ctx.helpers.replyTo(payload.exchangeName,payload.replyTo,cmd)
      } yield ()
      case None => ctx.logger.error(Errors.FileNotFound(payload.fileId).message)
    }
  } yield ()

  override def handle(): IO[Unit] = handler(command.payload.as[Payloads.DownloadFile])
}
object DownloadFileHandler {
  def apply(command: Command[Json])(implicit ctx:NodeContext[IO]): IO[Unit] =
    new DownloadFileHandler(command).handle()
}

