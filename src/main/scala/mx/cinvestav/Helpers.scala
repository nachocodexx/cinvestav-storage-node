package mx.cinvestav
import java.io.FileOutputStream
import java.net.URL
import java.nio.channels.Channels
import java.nio.channels.ReadableByteChannel
import cats.implicits._
import cats.effect.IO
import mx.cinvestav.config.DefaultConfig

object Helpers {

  def saveFile(filename:String, url:String, position:Int=0)(implicit config:DefaultConfig):IO[Long]= {
    val website = new URL(url)
    val rbc = Channels.newChannel(website.openStream)
    val fos = new FileOutputStream(s"${config.storagePath}/$filename")
    IO.delay{fos.getChannel.transferFrom(rbc, position, Long.MaxValue)}
  }

}
