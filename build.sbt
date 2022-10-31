ThisBuild / version := "0.5.1"

ThisBuild / scalaVersion := "2.12.10"

val sparkVersion = "3.1.2"
val akkaVersion = "2.5.26"
val akkaHttpVersion = "10.1.11"

resolvers += "jitpack" at "https://jitpack.io"

lazy val root = (project in file("."))
  .settings(
    name := "HorusDT",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-mllib" % sparkVersion,
      "mysql" % "mysql-connector-java" % "5.1.44",
      "com.github.shin285" % "KOMORAN" % "3.3.4",
      "com.influxdb" % "influxdb-client-scala_2.12" % "6.4.0",
      "com.typesafe.slick" %% "slick" % "3.3.2",
      "org.slf4j" % "slf4j-nop" % "1.6.4",
      "com.typesafe.slick" %% "slick-hikaricp" % "3.3.2",
      "com.mchange" % "c3p0" % "0.9.5.2",
      "com.typesafe" % "config" % "1.4.2",
      "org.jblas" % "jblas" % "1.2.5"
    )
  )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("reference.conf") => MergeStrategy.concat
  case PathList("application.conf") => MergeStrategy.concat
  case x => MergeStrategy.last
}
