package mx.cinvestav
import cats.data.EitherT

import java.io.{File, FileOutputStream}
import java.net.URL
import java.nio.channels.Channels
import java.nio.channels.ReadableByteChannel
import cats.implicits._
import cats.effect.{IO, Ref}
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.{ArrayVal, BooleanVal, StringVal}
import dev.profunktor.fs2rabbit.model.{AmqpMessage, AmqpProperties, ExchangeName, ExchangeType, RoutingKey}
import fs2.compression.ZLibParams.Header.GZIP
import fs2.concurrent.SignallingRef
import mx.cinvestav.Declarations.{DownloadError, NodeContextV5, NodeError, StorageNode}
import mx.cinvestav.Main.NodeContext
import mx.cinvestav.commons.balancer.LoadBalancer
import mx.cinvestav.domain.Errors.{CompressionFail, DecompressionFail, DuplicatedReplica, Failure, FileNotFound, RFGreaterThanAR}
import mx.cinvestav.commons.commands.{CommandData, Identifiers}
import mx.cinvestav.commons.payloads
import mx.cinvestav.commons.payloads.AddKey
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.domain.Constants.CompressionUtils
import mx.cinvestav.domain.{CommandId, NodeState, Payloads}
import mx.cinvestav.commons.storage.{FileMetadata, Replica}
import mx.cinvestav.commons.compression
import mx.cinvestav.commons.fileX.{FileMetadata => Metadata}
import mx.cinvestav.commons.types.Location
import mx.cinvestav.utils.RabbitMQUtils
import mx.cinvestav.utils.v2.{PublisherConfig, PublisherV2, RabbitMQContext}
import org.typelevel.log4cats.Logger

import javax.print.attribute.standard.Destination
import scala.util.Try
//
import io.circe.Json
import io.circe.syntax._
import io.circe.generic.auto._
import io.circe.{Decoder, Json}
import scala.concurrent.duration._
import scala.language.postfixOps
import mx.cinvestav.utils.PublisherNode
import com.github.gekomad.scalacompress.Compressors._
import com.github.gekomad.scalacompress.CompressionStats
import com.github.gekomad.scalacompress.DecompressionStats
import mx.cinvestav.utils.v2.encoders._
import org.apache.commons.io.FileUtils
import mx.cinvestav.commons.payloads.{v2=>PAYLOADS}

class Helpers()(implicit utils: RabbitMQUtils[IO],config: DefaultConfig,logger: Logger[IO]){



  //  ________________________________________________________________________________
  def selectLoadBalancer(defaultLoadBalancer:LoadBalancer,newLoadBalancer:String)(implicit ctx:NodeContext[IO]): LoadBalancer =
    Option.when(newLoadBalancer.trim.isEmpty || newLoadBalancer.trim.toUpperCase=="DEFAULT")(defaultLoadBalancer)
      .getOrElse(defaultLoadBalancer.changeBalancer(x=newLoadBalancer.trim.toUpperCase,reset = false))

  def replyTo(exchangeName:String,replyTo:String,cmd:CommandData[Json])(implicit ctx:NodeContext[IO]):IO[Unit] = for {
    _ <- IO.unit
    maybeExchangeName = Option.unless(exchangeName.isEmpty)(exchangeName)
    maybeReplyTo      = Option.unless(replyTo.isEmpty)(replyTo)
    _ <- maybeExchangeName.mproduct(_ => maybeReplyTo) match {
      case Some(value) =>for {
        publisher <- ctx.utils.createPublisher(value._1,value._2)
//        _        <- publisher(cmd.asJson.noSpaces)
      } yield ()
      case None => IO.unit
    }
  }  yield ()
  def addReplicas(fileId:String, newReplicas: List[Replica], state:Ref[IO,NodeState]): IO[Unit] = for {
    oldMetadata <- state.get.map(_.metadata)
    maybeFileMetadata = oldMetadata.get(fileId)
    _                 <- if(maybeFileMetadata.isDefined) for {
      _            <- IO.unit
      fileMetadata = maybeFileMetadata.get
      replicas     = Set.from(fileMetadata.replicas ++ newReplicas).toList
      newMetadata  = fileMetadata.copy(replicas = replicas )
      _            <- state.update(s=> s.copy(metadata =   oldMetadata.updated(fileId, newMetadata)   ))
    } yield ()
    else Logger[IO].error(FileNotFound(fileId).message)
//    _           <- if(maybeFileMetadata.isDefined) IO.unit else IO.unit
  } yield ()

  def activeReplication(payload:payloads.UploadFile, metadata: FileMetadata)(implicit ctx:NodeContext[IO]): IO[Unit] = for {
    currentState  <- ctx.state.get
//     _            <- Logger[IO].info(s"ACTIVE_REPLICATION ${payload.id} ${payload.fileId} ${payload.experimentId}")
     loadBalancer <-  currentState.loadBalancer.pure[IO]
     storageNodes <- currentState.storagesNodes.pure[IO]
     _            <- if(storageNodes.length < payload.replicationFactor)
                                Logger[IO].error(RFGreaterThanAR().message)
              else for {
                _ <- IO.unit
//                LOAD BALANCER
                loadBalancerA        = selectLoadBalancer(currentState.loadBalancer,payload.loadBalancer)
                selectedStorageNodes <- loadBalancerA.balanceMulti(storageNodes,rounds=payload.replicationFactor).pure[IO]
                _                    <- ctx.state.update(s=>s.copy(loadBalancer = loadBalancerA.changeBalancer(currentState.loadBalancer.getAlgorithm)))
//               END LOAD BALANCER
//                CHORD
                _                    <- ctx.state.update(s=>s.copy(activeReplicationCompletion = s.activeReplicationCompletion+(payload.fileId->selectedStorageNodes.length)))
//
                publishers <- selectedStorageNodes.traverse(nId =>
                  utils.fromNodeIdToPublisher(
                    nodeId       = nId,
                    exchangeName = config.poolId,
                    routingKey   = s"${config.poolId}.$nId.default"
                  )
                )
                compressionAlgo =  compression.fromString(payload.compressionAlgorithm.trim.toUpperCase)
                compressionExt  = compressionAlgo.extension
                _payload = Payloads.ActiveReplication(
                    id           = payload.id,
                    metadata     =  metadata,
                    userId       = payload.userId,
                    fileId       = payload.fileId,
                    url          = s"http://${currentState.ip}/${payload.fileId}.$compressionExt",
                    leaderNodeId = config.nodeId,
                    experimentId = payload.experimentId
                  )
                cmd      = CommandData[Json](CommandId.ACTIVE_REPLICATION,_payload.asJson).asJson.noSpaces
                _        <- publishers.traverse{publisher=>
                  publisher.publish(cmd) *> Logger[IO].info(s"ACTIVE_REPLICATION ${payload.id} ${publisher.nodeId} ${payload.experimentId}")
                }
              } yield ()
  } yield ()

  def saveAndCompress(payload: payloads.UploadFile, maybeMeta:Option[FileMetadata])(implicit ctx:NodeContext[IO]):EitherT[IO, Failure, FileMetadata] =
    if(maybeMeta.isDefined) EitherT.fromEither[IO](Left(DuplicatedReplica(payload.fileId)))
    else for {
      file             <- ctx.helpers.saveFileE(payload)
      _                <-Logger.eitherTLogger[IO,Failure].debug(s"COMPRESSION_INIT ${payload.id} ${payload.fileId} " +
        s"${payload.experimentId}")
      cs               <- ctx.helpers.compressEIO(file.getPath,s"${config.storagePath}",payload.compressionAlgorithm.trim.toUpperCase())
      _                <- Logger.eitherTLogger[IO,Failure]
        .debug(s"COMPRESSION_DONE ${payload.id} ${payload.fileId} ${cs.method} ${cs.millSeconds} ${cs.sizeIn} ${cs
          .sizeOut} ${cs.mbPerSecond} ${payload.experimentId}")
      metadata         <- ctx.helpers.createFileMetadataE(payload,file)
      _                <- EitherT.fromEither[IO](file.delete().asRight)
    } yield  metadata

  def createFileMetadataE(payload: payloads.UploadFile,file: File):EitherT[IO, Failure, FileMetadata] = {
      val fileMetadata:EitherT[IO,Failure,FileMetadata] = for {
        timestamp     <- EitherT(IO.realTime.map(x=>(x.toSeconds/1000L).asRight))
        replica       = Replica(config.nodeId,primary = true,0,timestamp)
        compressionAlgo = compression.fromString(payload.compressionAlgorithm)
        fileMetadata  <- EitherT[IO,Failure,FileMetadata](
          FileMetadata(
            originalName         = payload.filename,
            originalExtension    = payload.extension,
            compressionAlgorithm = compressionAlgo.token,
            size                 = file.length(),
            replicas             = replica::Nil
            //            compressionExt = "lz4"
          ).asRight[Failure].pure[IO]
        )
      } yield fileMetadata
      fileMetadata
  }


  def buildPassiveReplication(payload:payloads.UploadFile,metadata: FileMetadata,loadBalancer: LoadBalancer)(implicit ctx:NodeContext[IO]): IO[Unit] = for {
    ip <- ctx.state.get.map(_.ip)
    ext = CompressionUtils.getExtensionByCompressionAlgorithm(payload.compressionAlgorithm)
    passiveRepPayload = Payloads.PassiveReplication(
      id=payload.id,
      userId = payload.userId,
      fileId = payload.fileId,
      metadata =metadata,
      replicationFactor = payload.replicationFactor,
      url= s"http://$ip/${payload.fileId}.$ext",
      lastNodeId = ctx.config.nodeId,
      experimentId = payload.experimentId,
      loadBalancer = loadBalancer.getAlgorithm
    )
    _ <- _passiveReplication(Nil,passiveRepPayload)
  } yield ()
  def _passiveReplication(replicaNodes:List[String],newPayload:Payloads.PassiveReplication)(implicit ctx:NodeContext[IO]): IO[Unit] = for {
    currentState   <- ctx.state.get
    lb             =  currentState.loadBalancer
    storageNodes   = currentState.storagesNodes
    availableNodes = storageNodes.toSet.diff(replicaNodes.toSet).toList
    cmd            = CommandData[Json](CommandId.PASSIVE_REPLICATION,newPayload.asJson).asJson.noSpaces
    nextNodeId     <- lb.balance(availableNodes,Map.empty[String,Int]).pure[IO]
    publisher      <- utils.fromNodeIdToPublisher(nextNodeId,config.poolId,s"${config.poolId}.$nextNodeId.default")
    _              <- publisher.publish(cmd)
    _              <- ctx.logger.debug(s"CONTINUE_PASSIVE_REPLICATION ${newPayload.id} $nextNodeId ${newPayload.experimentId}")

  } yield ()


  def _startHeart(heartbeatSignal:SignallingRef[IO,Boolean]): IO[Unit] =  for {
    _               <- heartbeatSignal.set(false)
    heartbeatQueue  <- s"${config.poolId}-heartbeat".pure[IO]
    heartbeatRk     <- s"${config.poolId}.heartbeat".pure[IO]
    _               <- utils.createQueue(heartbeatQueue,config.poolId,ExchangeType.Topic,heartbeatRk)
    //        HEARTBEAT PUBLISHER
    heartbeatPublisher <- utils.createPublisher(config.poolId,heartbeatRk)
//    _                  <- utils
//      .publishEvery(this.heartbeat(_,heartbeatPublisher),config.heartbeatTime milliseconds)
//      .interruptWhen(heartbeatSignal)
//      .compile.drain.start
  } yield ()

  def heartbeat(value:Int,publisher:String=>IO[Unit]):IO[Unit] =
    for {
      beat     <- payloads.HeartbeatPayload(value=value,config.nodeId).pure[IO]
      beatJson <- beat.asJson.pure[IO]
      command  <- CommandData[Json](CommandId.HEARTBEAT,payload  = beatJson).pure[IO]
      _        <- publisher(command.asJson.noSpaces)
      _        <- Logger[IO].trace(s"HEARTBEAT ${beat.value} ${config.nodeId}")
    } yield ()


  def transferE(filename:String, fos:FileOutputStream,rbc:ReadableByteChannel): EitherT[IO, Failure, Long] =
    EitherT(
      fos.getChannel.transferFrom(rbc,0,Long.MaxValue)
        .pure[IO]
        .map(_.asRight[Failure])
        .handleErrorWith{ t =>
          val res:Either[Failure,Long] = FileNotFound(filename).asLeft[Long]
          IO.pure(res)
        }
    )

  def newChannelE(filename:String,url: URL): Either[FileNotFound, ReadableByteChannel] = Either.fromTry(
    Try {
      Channels.newChannel(url.openStream())
    }
  )
    .flatMap(_.asRight[Failure])
    .leftFlatMap{ t=>
   Left(FileNotFound(filename))
  }

  def downloadFileFormURL(fileId:String,filePath:String,url: URL): EitherT[IO, Failure, Long] = for {
    rbc           <- EitherT.fromEither[IO](newChannelE(fileId,url))
    fos           = new FileOutputStream(filePath)
//    _ <- url.getPath
    transferred   <- transferE(fileId,fos,rbc)
  } yield transferred

  def saveFileE(payload:payloads.UploadFile): EitherT[IO,Failure,File] = {
    for {
      _             <- Logger.eitherTLogger[IO,Failure].debug(s"SAVE_FILE_INIT ${payload.id} ${payload.fileId} " +
        s"${payload.experimentId}")
      completeUrl   <- EitherT.fromEither[IO](s"${payload.url}/${payload.filename}.${payload.extension}".asRight)
      website       =   new URL(completeUrl)
      filePath      = s"${config.storagePath}/${payload.fileId}"
      transferred   <- downloadFileFormURL(payload.fileId,filePath,website)
      _             <- Logger.eitherTLogger[IO,Failure].debug(s"SAVE_FILE_DONE ${payload.id} ${payload.fileId} $transferred ${payload.experimentId}")
      file          <- EitherT.fromEither[IO](Right(new File(filePath)))
    } yield file
  }

  def decompressE(src:String,destination:String): Either[Failure, DecompressionStats] = {
    Either.fromTry(lz4Decompress(src, destination))
    .leftFlatMap(t=>Left(DecompressionFail(t.getMessage)))
  }
  def decompressEIO(src:String,destination:String): EitherT[IO, Failure, DecompressionStats] =
    EitherT.fromEither[IO](decompressE(src,destination))

  def compressE(src:String,destination:String):Either[Failure,CompressionStats] =
    Either.fromTry(lz4Compress(src,destination))
      .leftFlatMap(t=>Left(CompressionFail(t.getMessage)))


  type CompressionFn = (String,String)=>Try[CompressionStats]
  def _compressE(tFn:CompressionFn)(src:String,destination:String):EitherT[IO,Failure,CompressionStats] =
    EitherT.fromEither[IO](Either.fromTry(tFn(src,destination)).leftFlatMap(t=>Left(CompressionFail(t.getMessage))))

  def compressEIO(src:String,destination:String,compressionAlgorithm:String): EitherT[IO, Failure, CompressionStats] = {
    compressionAlgorithm match {
      case compression.LZ4  =>  _compressE(lz4Compress)(src,destination)
      case compression.GZIP => _compressE(gzCompress)(src,destination)
      case _      => _compressE(gzCompress)(src,destination)
    }

  }


  def saveFile(filename:String, url:String, position:Int=0): IO[File] =
    for {
      _         <- Logger[IO].debug("SAVE_FILE")
       website  =   new URL(url)
       rbc      = Channels.newChannel(website.openStream)
       filePath = s"${config.storagePath}/$filename"
       fos      = new FileOutputStream(filePath)
      _        <- IO.delay(fos.getChannel.transferFrom(rbc,position,Long.MaxValue))
      file     <- IO.pure(new File(filePath))
    } yield  file

  def sendMetadataToChord(cmd:CommandData[Json])(implicit ctx:NodeContext[IO]):IO[Unit ] = for {
    currentState <- ctx.state.get
//    addKeyPayload = AddKey(id=payload.id,
//      key=payload.fileId,
//      value = payload.metadata.asJson.noSpaces,
//      experimentId = 0
//    )
    chordPublisher  <- ctx.utils.createPublisher(
      exchangeName = ctx.config.poolId,
      routingKey = currentState.chordRoutingKey
    )
//    addKeyCmd = CommandData[Json](Identifiers.ADD_KEY,addKeyPayload.asJson).asJson.noSpaces
//    _ <- chordPublisher(cmd.asJson.noSpaces)
  } yield ()
}

object Helpers {
  def apply()(implicit utils: RabbitMQUtils[IO],config: DefaultConfig,logger: Logger[IO]) = new Helpers()

  //  ________________________________________________________________________________
  def replicationCompleted(taskId:String,chunkId:String,location:Location)(implicit ctx:NodeContextV5) = for {

    currentState <- ctx.state.get
    lb           = currentState.loadBalancerPublisher
    timestamp    <- IO.realTime.map(_.toMillis)
    nodeId       = ctx.config.nodeId
    properties   = AmqpProperties(headers = Map("commandId" -> StringVal("REPLICATION_COMPLETED") ))

    msgPayload = PAYLOADS.ReplicationCompleted(
      replicationTaskId = taskId,
      storageNodeId     = nodeId,
      chunkId           = chunkId,
      //        uploadFileOutput.metadata.filename.value,
      timestamp         = timestamp,
      location          = location
      //        Location(url=url,source =source.toString)
    ).asJson.noSpaces
    msg      = AmqpMessage[String](payload = msgPayload,properties = properties)
    _ <- lb.publish(msg)
  } yield ()

  def fromStorageNodeToPublisher(x:StorageNode)(implicit rabbitMQContext:RabbitMQContext): PublisherV2 = {
    val exchange = ExchangeName(x.poolId)
    val routingKey = RoutingKey(s"${x.poolId}.${x.nodeId}")
    val cfg = PublisherConfig(exchangeName = exchange,routingKey = routingKey)
    PublisherV2.create(x.nodeId,cfg)
  }

  def downloadFromURL(url:URL,destination: File): Either[DownloadError, Unit] = {
    try{
        Right(FileUtils.copyURLToFile(url,destination))
    } catch {
      case ex: Throwable => Left(DownloadError(ex.getMessage))
    }
  }

  def transferE(filename:String, fos:FileOutputStream,rbc:ReadableByteChannel): EitherT[IO, Failure, Long] =
    EitherT(
      fos.getChannel.transferFrom(rbc,0,Long.MaxValue)
        .pure[IO]
        .map(_.asRight[Failure])
        .handleErrorWith{ t =>
          val res:Either[Failure,Long] = FileNotFound(filename).asLeft[Long]
          IO.pure(res)
        }
    )

  def newChannelE(filename:String,url: URL): Either[FileNotFound, ReadableByteChannel] = Either.fromTry(
    Try {
      Channels.newChannel(url.openStream())
    }
  )
    .flatMap(_.asRight[Failure])
    .leftFlatMap{ t=>
      Left(FileNotFound(filename))
    }


  def downloadFileFormURLV5(fileId:String, outputPath:String, url: URL): EitherT[IO, DownloadError, Long] = for {
    rbc           <- EitherT.fromEither[IO](newChannelE(fileId,url))
      .leftMap(e=>DownloadError(e.message))
    fos           = new FileOutputStream(outputPath)
    //
    transferred   <- transferE(fileId,fos,rbc)
      .leftMap(e=>DownloadError(e.message))
  } yield transferred

  def passiveReplicationV5(payload: Payloads.UploadV5,metadata: Metadata)(implicit ctx:NodeContextV5): IO[Unit] = {
    for {
      currentState <- ctx.state.get
      nodeId       = ctx.config.nodeId
      storagePath  = ctx.config.storagePath
      fullname     = metadata.fullname
//      /data/FILE_ID.LZ4
      filePath     = s"$storagePath/$fullname"
      ip           = currentState.ip
      port         = ctx.config.port
      _            <- ctx.logger.info("PASSIVE_REPLICATION")
      newPayload = payload.copy(
//        replicationFactor = payload.replicationFactor-1,
        url = s"http://$ip:$port/$filePath"
      )
      properties = AmqpProperties(
        headers = Map(
          "commandId"->StringVal(Identifiers.UPLOAD_FILE),
          "storageNodes" -> ArrayVal(Vector( StringVal(nodeId)  ) )
        )
      )
      message    = AmqpMessage[String](payload=newPayload.asJson.noSpaces,properties =properties )
//      _ <-
    } yield ()
  }

  def activeReplicationV5(payload: Payloads.UploadV5, storageNodes:List[PublisherV2])(implicit ctx:NodeContextV5): IO[Unit] = {

    for {
      _ <- ctx.logger.debug(s"ACTIVE_REPLICATION")
      props = AmqpProperties(
        headers = Map(
          "commandId" -> StringVal(Identifiers.UPLOAD_FILE),
          "isSlave"   -> BooleanVal(true),
        ),
        replyTo = ctx.config.nodeId.some
      )
      message = AmqpMessage(payload=payload.asJson.noSpaces,properties = props)
      _ <- ctx.logger.info(storageNodes.mkString(","))
      _ <- storageNodes.traverse(_.publish(message))
    } yield ()
  }
}
