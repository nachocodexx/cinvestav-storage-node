package mx.cinvestav.domain

object Payloads {
  case class DownloadFile(fileId:String,replyTo:String)
  case class UploadFile(
                         id:String,
                         fileId:String,
                         filename:String,
                         extension:String,
                         userId:String,
                         url:String,
                         replication_factor:Int,
//                         replicas:Int,nodes:List[String],
//                         compression:Boolean,
                         compressionAlgorithm:String = "lz4"
                       )
  case class UpdateReplicationFactor(replicationFactor:Int)

  case class Replication(
                          id:String,
                          fileId:String,
                          //                          compressionAlgorithm:
                          //                          filename:String,
                          extension:String,
                          userId:String,
                          url:String,
                          originalFilename:String,
                          originalExtension:String,
                          originalSize:Long,
                          replication_factor:Int,
                          compressionAlgorithm:String,
                          nodes:List[String]
                        )


//  case class NewCoordinator(prev:String,current:String)
}
