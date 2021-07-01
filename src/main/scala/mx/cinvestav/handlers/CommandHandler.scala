package mx.cinvestav.handlers

import cats.effect.{IO, Ref}
import io.circe.{Decoder, DecodingFailure, Json}
import mx.cinvestav.domain.NodeState
import mx.cinvestav.utils.Command

trait CommandHandler[F[_],P] {
  def handleLeft(df:DecodingFailure):F[Unit]
  def handleRight(payload:P):F[Unit]
  def handle():F[Unit]

  def handler(dr:Decoder.Result[P]): F[Unit] = dr match {
    case Left(value) => handleLeft(value)
    case Right(value) => handleRight(value)
  }
}
