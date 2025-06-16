name := "java-kcl-worker"

ThisBuild / scalaVersion := "2.13.15"
scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-Ywarn-value-discard",
  "-Ywarn-unused"
)
Compile / console / scalacOptions -= "-Ywarn-unused"

libraryDependencies ++= Seq(
  // intentionally held at v2, not v3
  "software.amazon.kinesis" % "amazon-kinesis-client" % "2.6.1",
  "org.slf4j" % "slf4j-simple" % "2.0.16" % Runtime
)

assembly / mainClass := Some("test.Worker")
assembly / assemblyJarName := "java-kcl-worker.jar"
assembly / target := file("./")
assemblyMergeStrategy := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.first
}
