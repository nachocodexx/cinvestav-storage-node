package mx.cinvestav.domain

object Constants {
  object ReplicationStrategies{
    final val PASSIVE = "passive"
    final val ACTIVE = "active"
  }

  object CompressionUtils {
    def getExtensionByCompressionAlgorithm(algorithm:String): String = algorithm match {
      case "lz4" => "lz4"
      case "LZ4" => "lz4"
      case _ => ""
    }
  }

}
