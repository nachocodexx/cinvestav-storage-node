package mx.cinvestav
import java.io.FileOutputStream
import java.net.URL
import java.nio.channels.Channels
import java.nio.channels.ReadableByteChannel
import cats.implicits._
import cats.effect.IO
import mx.cinvestav.commons.commands.CommandData
import mx.cinvestav.commons.payloads
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.domain.CommandId
import mx.cinvestav.utils.RabbitMQUtils
//
import io.circe.Json
import io.circe.syntax._
import io.circe.generic.auto._
import io.circe.{Decoder, Json}


object Helpers {

  def heartbeat(value:Int,publisher:String=>IO[Unit])(implicit utils:RabbitMQUtils[IO]):IO[Unit] = for {
    _       <- IO.println("HEART BEAT.... <3")
    beat    <- payloads.HeartbeatPayload(value=value).pure[IO].map(_.asJson)
    _       <- IO.println(beat)
    command <- CommandData[Json](CommandId.HEARTBEAT,payload  = beat).pure[IO]
    _       <- publisher(command.asJson.noSpaces)
  } yield ()
  def saveFile(filename:String, url:String, position:Int=0)(implicit config:DefaultConfig):IO[Long]= {
    val website = new URL(url)
    val rbc = Channels.newChannel(website.openStream)
    val fos = new FileOutputStream(s"${config.storagePath}/$filename")
    IO.delay{fos.getChannel.transferFrom(rbc, position, Long.MaxValue)}
  }

}
