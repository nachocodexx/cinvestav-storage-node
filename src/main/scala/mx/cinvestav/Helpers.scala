package mx.cinvestav
import java.io.FileOutputStream
import java.net.URL
import java.nio.channels.Channels
import java.nio.channels.ReadableByteChannel
import cats.implicits._
import cats.effect.IO
import dev.profunktor.fs2rabbit.model.ExchangeType
import fs2.concurrent.SignallingRef
import mx.cinvestav.commons.commands.CommandData
import mx.cinvestav.commons.payloads
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.domain.CommandId
import mx.cinvestav.utils.RabbitMQUtils
import org.typelevel.log4cats.Logger
//
import io.circe.Json
import io.circe.syntax._
import io.circe.generic.auto._
import io.circe.{Decoder, Json}
import scala.concurrent.duration._
import scala.language.postfixOps


object Helpers {

  def _startHeart(heartbeatSignal:SignallingRef[IO,Boolean])(implicit utils: RabbitMQUtils[IO],config: DefaultConfig,
                                                             logger: Logger[IO]
  ): IO[Unit] =  for {
    _               <- heartbeatSignal.set(false)
    heartbeatQueue  <- s"${config.poolId}-heartbeat".pure[IO]
    heartbeatRk     <- s"${config.poolId}.heartbeat".pure[IO]
    _               <- utils.createQueue(heartbeatQueue,config.poolId,ExchangeType.Topic,heartbeatRk)
    //        HEARTBEAT PUBLISHER
    heartbeatPublisher <- utils.createPublisher(config.poolId,heartbeatRk)
    _                  <- utils
      .publishEvery(Helpers.heartbeat(_,heartbeatPublisher),config.heartbeatTime milliseconds)
      .interruptWhen(heartbeatSignal)
      .compile.drain.start
  } yield ()
  def heartbeat(value:Int,publisher:String=>IO[Unit])(implicit utils:RabbitMQUtils[IO],config: DefaultConfig,logger: Logger[IO])
  :IO[Unit] =
    for {
    beat     <- payloads.HeartbeatPayload(value=value,config.nodeId).pure[IO]
    beatJson <- beat.asJson.pure[IO]
    command  <- CommandData[Json](CommandId.HEARTBEAT,payload  = beatJson).pure[IO]
    _        <- publisher(command.asJson.noSpaces)
    _        <- Logger[IO].debug(s"HEARTBEAT ${beat.value} ${config.nodeId}")
  } yield ()
  def saveFile(filename:String, url:String, position:Int=0)(implicit config:DefaultConfig):IO[Long]= {
    val website = new URL(url)
    val rbc = Channels.newChannel(website.openStream)
    val fos = new FileOutputStream(s"${config.storagePath}/$filename")
    IO.delay{fos.getChannel.transferFrom(rbc, position, Long.MaxValue)}
  }

}
