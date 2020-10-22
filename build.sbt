lazy val root = (project in file(".")).
  settings(
    name := "karanvenkatesh_davanam_cs441_hw2",
    version := "0.1",
    scalaVersion := "2.13.3",
    mainClass in Compile := Some("cs441.JobsDriver"),
    mainClass in assembly := Some("cs441.JobsDriver")
  )

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.2",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.scalatest" %% "scalatest" % "3.1.1" % "test",
  "org.apache.hadoop" % "hadoop-common" % "3.1.1",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.1.1",
  "org.scala-lang.modules" %% "scala-xml" % "1.2.0"
)

assemblyJarName in assembly := "hadoop-map-reduce-scala-sbt-assembly-fatjar-1.0.jar"

// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}