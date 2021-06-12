package mx.cinvestav

import cats.effect.{IO, Ref}
import io.circe.generic.semiauto.deriveDecoder
import mx.cinvestav.domain.Payloads
import io.circe.{Decoder, Json}
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.utils.Command

object CommandProcessor {
  implicit val downloadFilePayloadDecoder:Decoder[Payloads.DownloadFilePayload] = deriveDecoder
  implicit val updateReplicationFactorPayloadDecoder:Decoder[Payloads.UpdateReplicationFactor] = deriveDecoder

  def updateReplicationFactor(command: Command[Json],state:Ref[IO,Map[String,Any]]): IO[Unit] = {
    val routingKey =command.envelope.routingKey.value
    state.getAndUpdate{ nodeState =>
      val currentRF = nodeState.getOrElse("rf", 1).asInstanceOf[Int]
      nodeState.updated("rf",currentRF+1)
    }.flatMap(IO.println)
      .flatMap(_=>IO.println(command.envelope))
  }

  def downloadFile(command:Command[Json])(implicit config:DefaultConfig): IO[Unit] ={
    val payload = command.payload.as[Payloads.DownloadFilePayload]
    payload match {
      case Left(e) =>
        IO.println(e.getMessage())
      case Right(payload) =>
        Helpers.saveFile(payload.fileId,payload.url) *> IO.unit
    }
  }

}
