package mx.cinvestav.server

import cats.data.Kleisli
import cats.effect.IO
import mx.cinvestav.Declarations.NodeContextV5
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.server.Router
import org.http4s.{HttpRoutes, Request, Response}
import org.http4s.implicits._
import org.http4s.dsl.io._

import scala.concurrent.ExecutionContext.global

object HttpServer {

  private def httpApp()(implicit ctx:NodeContextV5): Kleisli[IO, Request[IO],
    Response[IO]] =
    Router[IO](
      "/" -> Routes(),
    ).orNotFound

  def run()(implicit ctx:NodeContextV5): IO[Unit] = for {
    _ <- ctx.logger.debug(s"HTTP SERVER AT ${ctx.config.host}:${ctx.config.port}")
    _ <- BlazeServerBuilder[IO](executionContext = global)
    .bindHttp(ctx.config.port,ctx.config.host)
    .withHttpApp(httpApp = httpApp())
    .serve
    .compile
    .drain
  } yield ()

}
