package mx.cinvestav.server

import cats.effect.IO
import fs2.io.file.Files
import mx.cinvestav.Declarations.NodeContextV5
import org.http4s._
import org.http4s.implicits._
import org.http4s.dsl.io._

import java.io.File
import io.circe._
import io.circe.generic.auto._
import mx.cinvestav.commons.fileX.FileMetadata
import org.http4s.circe.CirceEntityDecoder._

import java.nio.file.Paths

object Routes {
//  case class DownloadPayload(id:String, extension:String)
  case class DownloadPayload(source:String)
  def hello(implicit ctx:NodeContextV5): HttpRoutes[IO] = HttpRoutes.of[IO]{
    case req@GET -> Root   =>
      Ok(s"Hello I'm ${ctx.config.nodeId}")
    case req@POST -> Root / "download"  => for{
      payload    <- req.as[DownloadPayload]
      source     = payload.source
      path       = Paths.get(source)
      file       = path.toFile
      response   <- if(!file.exists())  NotFound()
      else for {
        _        <- ctx.logger.debug(s"FILE_PATH $path")
        _        <- ctx.logger.debug(s"FILE_SIZE ${file.length()} bytes")
        metadata = FileMetadata.fromPath(path)
        _        <- ctx.logger.debug(s"FILE_EXTENSION ${metadata.extension}")
        bytes    = Files[IO].readAll(path,4098)
        _        <- ctx.logger.info(s"DOWNLOAD ${metadata.fullname} ${metadata.size.value.get}")
        response <- Ok(bytes)
      } yield response
    } yield response
  }
//  def hello(implicit ctx:NodeContextV5): HttpRoutes[IO] = HttpRoutes.of[IO]{
//    case req@GET -> Root   =>
//      Ok(s"Hello I'm ${ctx.config.nodeId}")
//    case req@POST -> Root / "download" => for{
//      payload  <- req.as[DownloadPayload]
//      _        <- ctx.logger.debug(s"DOWNLOAD_FILE ${payload.id}")
//      _        <- ctx.logger.debug(payload.toString)
////      ___________________________________________________________________________________
//      extension = if(payload.extension.isEmpty) "" else s".${payload.extension}"
//      filepath = s"${ctx.config.storagePath}/${ctx.config.nodeId}/${payload.id}${extension}"
//      file     = new File(filepath)
//      _        <- ctx.logger.debug(file.toString)
////      ___________________________________________________________________________
//      response <- if(!file.exists()) NotFound()
//      else for {
//          response <- Ok(Files[IO].readAll(file.toPath,4098))
//      } yield response
//
//    } yield response
//  }

  def apply()(implicit ctx:NodeContextV5): HttpRoutes[IO] = hello

}
