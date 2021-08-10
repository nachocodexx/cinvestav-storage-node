package mx.cinvestav

import cats.implicits._
import cats.effect.{ExitCode, IO, IOApp, Ref}
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model.{AMQPConnection, AmqpFieldValue, ExchangeName, ExchangeType, QueueName, RoutingKey}
import fs2.concurrent.SignallingRef
import mx.cinvestav.Declarations.{NodeContextV5, NodeStateV5, RabbitContext}
import mx.cinvestav.commons.commands.Identifiers
import mx.cinvestav.commons.{balancer, status}
import mx.cinvestav.config.{DefaultConfig, DefaultConfigV5}
import mx.cinvestav.domain.{CommandId, NodeState}
import mx.cinvestav.handlers.{ActiveReplicationDoneHandler, ActiveReplicationHandler, AddReplicasHandler, DownloadFileHandler, PassiveReplicationHandler, UploadHandler}
import mx.cinvestav.utils.RabbitMQUtils
import mx.cinvestav.utils.RabbitMQUtils.dynamicRabbitMQConfig
import mx.cinvestav.utils.v2.{Acker, Exchange, MessageQueue, PublisherConfig, PublisherV2}
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.ConfigSource
import pureconfig.generic.auto._

import java.io.File
import java.net.InetAddress

object MainV5 extends IOApp{
  implicit val config: DefaultConfigV5 = ConfigSource.default.loadOrThrow[DefaultConfigV5]
  val rabbitMQConfig: Fs2RabbitConfig  = RabbitMQUtils.parseRabbitMQClusterConfig(config.rabbitmq)
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]





  def mainProgram(queueName: QueueName)(implicit ctx:NodeContextV5) = {
    val connection = ctx.rabbitContext.connection
    val client = ctx.rabbitContext.client
    client.createChannel(connection) .use { implicit channel =>
      for {
        _ <- ctx.logger.debug("START CONSUMING")
        (_acker, consumer) <- ctx.rabbitContext.client.createAckerConsumer(queueName = queueName)
        _ <- consumer.evalMap { implicit envelope =>
          val maybeCommandId = envelope.properties.headers.get("commandId")
          implicit val acker: Acker = Acker(_acker)
          maybeCommandId match {
            case Some(commandId) => commandId match {
              case AmqpFieldValue.StringVal(value) if value== Identifiers.UPLOAD_FILE =>
                CommandHandlers.uploadV5()
            }
            case None =>           for{
              _ <- ctx.logger.error("NO COMMAND_ID PROVIED")
              _ <- acker.reject(envelope.deliveryTag)
            } yield ()
          }
        }.compile.drain
      } yield ()
    }
  }

  override def run(args: List[String]): IO[ExitCode] = {
    RabbitMQUtils.initV2[IO](rabbitMQConfig){ implicit client=>
      client.createConnection.use{ implicit connection=>
        for {
          _               <- Logger[IO].info(config.toString)
          _               <- Logger[IO].info(s"STORAGE NODE[${config.nodeId}] is up and running 🚀")
          heartbeatSignal <- SignallingRef[IO,Boolean](false)
          //        //
          rootFile        = new File("/")
          exchangeName   = ExchangeName(config.poolId)
          storageNodePublisher = config.storageNodes.map{
            sn =>
              (sn,PublisherConfig(exchangeName =exchangeName,routingKey = RoutingKey(s"${config.poolId}.$sn")))
          }.map{
            case (snId, config) => (snId,PublisherV2(config))
          }.toMap


          _initState      = NodeStateV5(
            status              = status.Up,
            heartbeatSignal     = heartbeatSignal,
            loadBalancer        = balancer.LoadBalancer(config.loadBalancer),
            replicationFactor   = config.replicationFactor,
            storagesNodes       = config.storageNodes,
            ip                  = InetAddress.getLocalHost.getHostAddress,
            availableResources  = config.storageNodes.length+1,
            replicationStrategy = config.replicationStrategy,
            freeStorageSpace    = rootFile.getFreeSpace,
            usedStorageSpace    = rootFile.getTotalSpace - rootFile.getFreeSpace,
            chordRoutingKey          = s"${config.poolId}.global.chord",
            storageNodesPubs =  storageNodePublisher
          )
          state           <- IO.ref(_initState)
          _ <- Exchange.topic(exchangeName = exchangeName)
          queueName = QueueName(s"${config.poolId}-${config.nodeId}")
          routingKey = RoutingKey(s"${config.poolId}.${config.nodeId}")
          rabbitContext =RabbitContext(client = client,connection=connection)
          _ <- MessageQueue.createThenBind(
            queueName = queueName,
            exchangeName=exchangeName,
            routingKey = routingKey
          )
                  ctx             = NodeContextV5(config,logger = unsafeLogger,state=state,rabbitContext = rabbitContext)
          _ <- mainProgram(queueName = queueName)(ctx=ctx)
        } yield ()
      }
    }
  }.as(ExitCode.Success)
}
