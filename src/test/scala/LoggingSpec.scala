import cats.data.EitherT
import cats.implicits._
import cats.effect._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.net.URL

class LoggingSpec extends munit .CatsEffectSuite {
  implicit def unsafeLogger = Slf4jLogger.getLogger[IO]
  test("URL"){
    val url = new URL("http://10.0.0.0/filename.lz4")
    val op = Option.unless("".isEmpty)("")
    val replicas = List(1,2,3)
    val newReplicas =List(1,2,3,4)
    val reps = Set.from(replicas++newReplicas)
    println(op,"".isEmpty)
//    println(url.getPath)
  }
  test("Basic") {
//    Logger.eitherTLogger[IO,Throwable]
    val a:EitherT[IO,Throwable,Int] = for {
      res <- EitherT[IO,Throwable,Int](5.asRight.pure[IO])
      _   <- EitherT.fromEither[IO](5.asRight)
      _   <- Logger.eitherTLogger[IO,Throwable].debug(s"HOLAAA => ${res}")
    } yield  res

    a.value.flatMap {
      case Left(value) =>
        Logger[IO].debug("LEFT!")
      case Right(value) =>
        Logger[IO].debug("RIGHT")
    }
  }

}
