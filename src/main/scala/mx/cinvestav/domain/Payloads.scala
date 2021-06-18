package mx.cinvestav.domain

object Payloads {
  case class DownloadFilePayload(fileId:String,url:String)
  case class UploadFile(fileId:String, url:String)
  case class UpdateReplicationFactor(replicationFactor:Int)
}
