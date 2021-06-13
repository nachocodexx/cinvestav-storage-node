package mx.cinvestav

import cats.implicits._
import cats.effect.{IO, Ref}
import dev.profunktor.fs2rabbit.model.ExchangeType
import mx.cinvestav.domain.{NodeState, Payloads}
import io.circe.generic.semiauto.deriveDecoder
import io.circe.{Decoder, Json}
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.domain.Payloads.UpdateReplicationFactor
import mx.cinvestav.utils.{Command, RabbitMQUtils}
//
import scala.concurrent.duration._
import scala.language.postfixOps

object CommandHandler {
  implicit val downloadFilePayloadDecoder:Decoder[Payloads.DownloadFilePayload] = deriveDecoder
  implicit val updateReplicationFactorPayloadDecoder:Decoder[Payloads.UpdateReplicationFactor] = deriveDecoder

  def stopHeartbeat(command: Command[Json],state:Ref[IO,NodeState])(implicit utils:RabbitMQUtils[IO],
                                                                    config: DefaultConfig) = for {
    _ <- IO.println("STOP HEART! </3")
    signal <- state.get.map(_.heartbeatSignal)
    _      <- signal.set(true)
    _      <- signal.set(false)
  } yield ()
  def startHeartbeat(command: Command[Json],state:Ref[IO,NodeState])(implicit utils:RabbitMQUtils[IO],config:DefaultConfig) =
    for {
      _               <- IO.println("START HEARTBEAT")
      heartbeatSignal <- state.get.map(_.heartbeatSignal)
      _               <- heartbeatSignal.set(false)
      heartbeatQueue  <- s"${config.poolId}-heartbeat".pure[IO]
      heartbeatRk     <- s"${config.poolId}.heartbeat".pure[IO]
      _               <- utils.createQueue(heartbeatQueue,config.poolId,ExchangeType.Topic,heartbeatRk)
      //        HEARTBEAT PUBLISHER
      heartbeatPublisher <- utils.createPublisher(config.poolId,heartbeatRk)
      _                  <- utils
        .publishEvery(Helpers.heartbeat(_,heartbeatPublisher),config.heartbeatTime seconds)
        .interruptWhen(heartbeatSignal)
        .compile.drain.start
    } yield ()

  def updateReplicationFactor(command: Command[Json],state:Ref[IO,NodeState]): IO[Unit] = {
    command.payload
      .as[UpdateReplicationFactor] match {
      case Left(e) =>
        IO.println(e.getMessage())
      case Right(payload) =>
        for {
           _ <- IO.println("UPDATE REPLICATION FACTOR")
          _ <- state.update(s=>s.copy(replicationFactor = payload.replicationFactor))
        } yield ()
    }
//    val routingKey =command.envelope.routingKey.value
//    state.getAndUpdate{ nodeState =>
////      val currentRF = nodeState.getOrElse("rf", 1).asInstanceOf[Int]
////      nodeState.updated("rf",currentRF+1)
//    }.flatMap(IO.println)
//      .flatMap(_=>IO.println(command.envelope))
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
