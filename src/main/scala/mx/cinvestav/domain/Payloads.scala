package mx.cinvestav.domain

object Payloads {
  case class FileFound(id:String,fileId:String,url:String,compressionAlgorithm:String)
  case class DownloadFile(id:String,fileId:String,exchangeName:String,replyTo:String)
  case class UploadFile(
                         id:String,
                         fileId:String,
                         filename:String,
                         extension:String,
                         userId:String,
                         url:String,
                         replicationFactor:Int,
//                         replicas:Int,nodes:List[String],
//                         compression:Boolean,
                         compressionAlgorithm:String = "lz4",
                         experimentId:Int
                       )
  case class UpdateReplicationFactor(replicationFactor:Int)

  case class ActiveReplicationDone(
                                    id:String,
                                    fileId:String,
                                    replica: Replica,
                                    transferred:Long,
                                    experimentId:Int
                                  )
  case class ActiveReplication(
                          id:String,
                          fileId:String,
//                          extension:String,
                              leaderNodeId:String,
                          userId:String,
                          url:String,
                          metadata:FileMetadata,
                          experimentId:Int,
                        )
  case class PassiveReplication(
                               id:String,
                               userId:String,
                               fileId:String,
                               metadata: FileMetadata,
                               replicationFactor:Int,
                               url:String,
                               lastNodeId:String,
                               experimentId:Int
                               )

  case class AddReplicas(id:String, fileId:String, replica: List[Replica],experimentId:Int)
  case class Replication(
                          id:String,
                          fileId:String,
                          extension:String,
                          userId:String,
                          url:String,
                          originalFilename:String,
                          originalExtension:String,
                          originalSize:Long,
                          replicationFactor:Int,
                          compressionAlgorithm:String,
                          nodes:List[String],
                          experimentId:Int,
//                          New
                          replicationStrategy:String
                        )


//  case class NewCoordinator(prev:String,current:String)
}
