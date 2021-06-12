import sbt._


object Dependencies {
  def apply(): Seq[ModuleID] = {
//    lazy val RabbitMQ = "dev.profunktor" %% "fs2-rabbit" % "4.0.0"
//    lazy val Logback    = "ch.qos.logback" % "logback-classic" % "1.2.3"
    lazy val RabbitMQUtils = "mx.cinvestav" %% "rabbitmq-utils" % "0.3.3"
    lazy val Commons = "mx.cinvestav" %% "commons" % "0.0.5"
    lazy val PureConfig = "com.github.pureconfig" %% "pureconfig" % "0.15.0"
    lazy val MUnitCats ="org.typelevel" %% "munit-cats-effect-3" % "1.0.3" % Test
    Seq(RabbitMQUtils,PureConfig,Commons,MUnitCats)
  }
}


