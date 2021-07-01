package mx.cinvestav.domain

object Errors {
  trait Failure{
    def message:String
  }
  case class FileNotFound(filename:String,message:String ="FILE_NOT_FOUND") extends Failure
  case class DuplicatedReplica(fileId:String,message:String="DUPLICATED_REPLICA") extends Failure
  case class CompressionFail(message:String) extends Failure
  case class DecompressionFail(message:String) extends Failure
  case class RFGreaterThanAR(message:String="RF(Replication factor) must be lower than AR(Available " +
    "resources) RF<AR") extends Failure
//  case class ARLower(message:String= "Available resources must be ")


}
