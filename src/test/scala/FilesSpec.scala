import cats.effect.IO
import cats.implicits._
import io.circe.Json
import mx.cinvestav.Helpers
import mx.cinvestav.commons.commands.CommandData
import mx.cinvestav.commons.payloads
import mx.cinvestav.commons.commands.Identifiers
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.utils.RabbitMQUtils

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
//
import pureconfig.generic.auto._
import pureconfig.ConfigSource
import io.circe._,io.circe.generic.auto._,io.circe.generic.semiauto._ ,io.circe.syntax._,io.circe.parser._
import scala.concurrent.duration._
import scala.language.postfixOps

class FilesSpec extends munit.CatsEffectSuite {
  implicit val config = ConfigSource.default.loadOrThrow[DefaultConfig]

  override def munitTimeout: Duration = Int.MaxValue seconds
  case class Testing(value:Int)
//  case class OkPayload(id:Int,nodeId:String)
//  case class ElectionPayload(nodeId:String,shadowNodeId:String)
  implicit val testDecoder:Decoder[Testing] =deriveDecoder
  implicit val testEncoder:Encoder[Testing] =deriveEncoder
  implicit val okEncoder:Encoder[payloads.Ok] =deriveEncoder
  implicit val electionEncoder:Encoder[payloads.Election] =deriveEncoder
  test("Concurrency"){
//    val es = Executors.newScheduledThreadPool(10)
//    val ec = ExecutionContext.fromExecutorService(es)
    for {
       _          <- IO.println("EXAMPLE")
       wait5Secs  = IO.sleep(5 seconds)
       wait1Sec   = IO.sleep(1 seconds)
       es         <- Executors.newScheduledThreadPool(10).pure[IO]
       threadPool <- ExecutionContext.fromExecutorService(es).pure[IO]
       f0         <- (wait5Secs *> IO.println("HELLO WORLD")).start
       f1         <- (wait1Sec *> IO.println("HELLO WORLD")).startOn(threadPool)
      _          <- IO.sleep(100 seconds)
    } yield ()
  }
  test("Json"){
    val t0 = Testing(0)
    IO.println(t0).flatMap{ _ =>
      val json = t0.asJson
      val str = json.noSpaces
      val str0 = "{\"value\":1}"
      IO.pure(decode(str0))
    }.flatMap(IO.println)
  }
  test("Basics".ignore) {
//    Helpers().saveFile("f-00.gif","http://localhost:6666/00.gif")
//      .flatMap(IO.println)
  }
  test("RabbitMQ"){
    RabbitMQUtils.init[IO](RabbitMQUtils.dynamicRabbitMQConfig(config.rabbitmq)) { implicit utils =>
      val app = for {
        _ <- IO.println("STARTING CONSUME!")
        _ <- utils.consumeJson("pool-xxxx-cs-yyyy")
          .evalMap{ command => command.commandId match {
            case Identifiers.OK =>
              IO.println("OK!!!!!!!!!!!!!")
            case Identifiers.ELECTIONS =>
              for {
                pub <- utils.createPublisher(config.poolId,s"${config.poolId}.cs-xxxx.default")
                cmd <- CommandData[Json](Identifiers.COORDINATOR,payloads.Coordinator("sn-yyyy","cs-yyyy").asJson).pure[IO ]
                okcmd <- CommandData[Json](Identifiers.OK,payloads.Ok("cs-yyyy").asJson).pure[IO]
                _ <- pub(cmd.asJson.noSpaces)
                _ <- pub(okcmd.asJson.noSpaces)
                _ <- IO.println("ELECTIONS!!!")
              } yield ()
           }

          }
          .compile.drain.start
        _ <- utils.consumeJson(queueName = "pool-xxxx-cs-zzzz")
          .evalMap{ command=>command.commandId match {
            case Identifiers.COORDINATOR =>
              IO.println(command.payload)
            case Identifiers.OK =>
              IO.println(command) *> IO.println("_____________")
          }

          }
          .compile.drain.start
      } yield ()
//      utils.createPublisher(config.poolId,s"${config.poolId}.${config.nodeId}.config")

     app *>  utils.createPublisher(config.poolId,s"${config.poolId}.cs-xxxx.default")
        .flatMap{ publisher =>
//          val command = CommandData("TEST",Json.Null)

//          val command = CommandData("OK",OkPayload(0,"cs-yyyy"))
          val command = CommandData(
            Identifiers.ELECTIONS,
            payloads.Election("sn-zzzz","cs-zzzz")
          )
          publisher(command.asJson.noSpaces)
//          publisher("{\"commandId\":\"REMOVE_NODE\",\"payload\":{\"nodeId\":\"sn-xxxx\"} }")
//          publisher("{\"commandId\":\"RESET_STATE\"}")
        } *> IO.sleep(1000 seconds)
    }
  }
}
