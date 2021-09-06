
lazy val app = (project in file(".")).settings(
  name := "storage-node",
  version := "0.1",
  scalaVersion := "2.13.6",
  libraryDependencies ++= Dependencies(),
  assemblyJarName := "storagenode.jar",
  Compile / run / mainClass := Some("mx.cinvestav.MainV5"),
  assembly / mainClass := Some("mx.cinvestav.MainV5"),
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
)
