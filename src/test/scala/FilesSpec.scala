import cats.data.EitherT
import cats.effect.IO
import cats.implicits._
import io.circe.Json
import mx.cinvestav.Helpers
import mx.cinvestav.commons.commands.CommandData
import mx.cinvestav.commons.{balancer, payloads}
import mx.cinvestav.commons.commands.Identifiers
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.domain.{CommandId, Payloads}
import mx.cinvestav.domain.Errors.Failure
import mx.cinvestav.utils.RabbitMQUtils
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.util.UUID
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
//
import pureconfig.generic.auto._
import pureconfig.ConfigSource
import io.circe._,io.circe.generic.auto._,io.circe.generic.semiauto._ ,io.circe.syntax._,io.circe.parser._
import scala.concurrent.duration._
import scala.language.postfixOps
import fs2.Stream
import scala.sys.process._


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
  val rabbitMQConfig = RabbitMQUtils.dynamicRabbitMQConfig(config.rabbitmq)
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  case class Command(command:Json)

  def workload(experimentId:Int,bullies:List[String],lb:balancer.LoadBalancer)(implicit H:Helpers): IO[Unit] =
    Stream.iterate(0)(_+1)
    .covary[IO]
    .evalMap{ i =>
      val node = lb.balance(bullies)
      val uploadPayload = Payloads.UploadFile(
        id     = s"op-$i",
        fileId = UUID.randomUUID().toString,
        filename= s"$i",
        extension = "pdf",
        userId = "user_00",
        url = "http://10.0.0.23",
        replicationFactor =  2,
        experimentId = experimentId
      )
      val uploadCmd = CommandData[Json](CommandId.UPLOAD_FILE,uploadPayload.asJson)
      val cmd = CommandData[Json]("RUN",payload = Command(uploadCmd.asJson).asJson)
      for {
        _         <-Logger[IO].debug(s"SEND file $i.pdf to $node")
        publisher <- H.fromNodeIdToPublisher(node,s"${config.poolId}.$node.default")
        _         <- publisher.publish(cmd.asJson.noSpaces)
      } yield ()
    }
    .metered(1100 milliseconds)
    .take(33)
    .compile.drain

  test("Workload") {
    val cmdRm =  """echo demonio0 | sudo -S rm /home/nacho/Documents/test/storage/sn-*/*"""
    val cmd = Seq("/bin/bash","-c",cmdRm)

    RabbitMQUtils.init[IO](rabbitMQConfig) { implicit utils =>
      implicit val H: Helpers = Helpers()
      val totalOfNodes = 3
      val nodesIds = 0 until totalOfNodes
      val storagesNodes = nodesIds.map(id=>s"sn-$id").toList
      val bullies = nodesIds.map(id=>s"cs-$id").toList
      val lb = balancer.LoadBalancer(config.loadBalancer)
      val MAX_EXPERIMENTS = 50
      Stream.iterate(0)(_+1)
        .covary[IO]
        .evalMap(i=>Logger[IO].debug(s"EXPERIMENT[$i] INIT") *>i.pure[IO])
        .evalMap{ experimentId=>
          for {
            _        <- workload(experimentId,bullies,lb)
            sns      <- storagesNodes.traverse(sn=>H.fromNodeIdToPublisher(sn,s"${config.poolId}.$sn.default"))
            resetCmd <- CommandData[Json](CommandId.RESET,Json.Null).asJson.noSpaces.pure[IO]
             _       <- sns.traverse(_.publish(resetCmd))
          } yield experimentId
        }
        .evalMap{i=>
          for {
            _ <- Logger[IO].debug(s"EXPERIMENT[$i] DONE")
          } yield ()
        }
        .take(MAX_EXPERIMENTS)
        .metered(1500 milliseconds)
        .compile.drain
//      workload(bullies,lb)

    }
  }

  test("Compression".ignore){
    RabbitMQUtils.init[IO](rabbitMQConfig){ implicit  utils =>
      val helpers = Helpers()
      val app = for {
        stats <-EitherT.fromEither[IO](
          helpers.compressE(
            src = "/home/nacho/Programming/Scala/storage-node/target/storage/01",
            destination = "/home/nacho/Programming/Scala/storage-node/target/storage")
        )
        stats01 <- EitherT.fromEither[IO](helpers.decompressE
        ("/home/nacho/Programming/Scala/storage-node/target/storage/01.lz4",
          "/home/nacho/Programming/Scala/storage-node/target/storage/decompress"))
        _     <- Logger.eitherTLogger[IO,Failure].debug(stats.toString)
        _     <- Logger.eitherTLogger[IO,Failure].debug(stats01.toString)
      } yield ( )
      app.value.flatMap {
        case Left(value) =>
          Logger[IO].debug(value.toString)
        case Right(value) =>
          Logger[IO].debug("SUCCESS")
      }
//        Logger[IO].debug("DONE!")
    }

  }

  test("Concurrency".ignore){
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
  test("Json".ignore){
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
  test("RabbitMQ".ignore){
    RabbitMQUtils.init[IO](rabbitMQConfig) { implicit utils =>
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
