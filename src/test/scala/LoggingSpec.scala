import cats.data.EitherT
import cats.implicits._
import cats.effect._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
class LoggingSpec extends munit .CatsEffectSuite {
  implicit def unsafeLogger = Slf4jLogger.getLogger[IO]
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
