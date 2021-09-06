import cats.data.EitherT
import cats.implicits._
import cats.effect._
import mx.cinvestav.commons.balancer.LoadBalancer
import mx.cinvestav.commons.fileX.FileMetadata
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.net.URL
import java.nio.file.Paths

class LoggingSpec extends munit .CatsEffectSuite {
  implicit def unsafeLogger = Slf4jLogger.getLogger[IO]
  test("URL"){
    val url = new URL("http://localhost:6666/download/my_file.lz4")
    val metadata = FileMetadata.fromPath(Paths.get(url.getPath))
    println(url,metadata)
  }
  test("Load balancer"){
    val payloadLoadBalancer =  "2C"
    val loadBalancer  = "RB"
    val value = Option.when(payloadLoadBalancer.trim.isEmpty || payloadLoadBalancer.trim.toUpperCase=="DEFAULT")(loadBalancer)
      .getOrElse(payloadLoadBalancer)
    val lb = LoadBalancer(value)
    lb.balance("sn-0"::"sn-1"::"sn-2"::Nil)
    lb.balance("sn-0"::"sn-1"::"sn-2"::Nil)
    lb.balance("sn-0"::"sn-1"::"sn-2"::Nil)
    println(value,lb.counter)
  }
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
