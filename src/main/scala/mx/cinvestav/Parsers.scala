package mx.cinvestav
import mx.cinvestav.commons.commands.CommandData
import mx.cinvestav.commons.payloads.HeartbeatPayload
import io.circe._
import io.circe.generic.semiauto.deriveEncoder

object Parsers {
  implicit val commandDataEncoder:Encoder[CommandData] =  deriveEncoder
 implicit val heartbeatPayloadEncoder:Encoder[HeartbeatPayload] =  deriveEncoder
}
