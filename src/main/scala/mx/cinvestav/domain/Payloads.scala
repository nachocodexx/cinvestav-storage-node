package mx.cinvestav.domain

object Payloads {
  case class DownloadFile(fileId:String, url:String)
  case class UploadFile(
                         id:String,
                         fileId:String,
                         filename:String,
                         extension:String,
                         userId:String,
                         url:String,
                         replicas:Int,nodes:List[String]
                       )
  case class UpdateReplicationFactor(replicationFactor:Int)
}
