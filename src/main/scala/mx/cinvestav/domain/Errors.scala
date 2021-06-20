package mx.cinvestav.domain

object Errors {
  trait Failure
  case class FileNotFound(filename:String) extends Failure
  case class DuplicatedReplica(fileId:String) extends Failure


}
