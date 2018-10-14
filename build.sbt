name := """amplifyr-server"""

version := "2.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.typesafe.play" %% "anorm" % "2.5.0"
)

libraryDependencies ++= Seq(
  cache,
  specs2 % Test,
  "mysql" % "mysql-connector-java" % "5.1.34",
  "com.typesafe.play" %% "play-slick" % "2.0.0",
  "com.typesafe.play" %% "play-slick-evolutions" % "2.0.0",
  "org.reactivemongo" %% "play2-reactivemongo" % "0.12.3",
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.apache.spark" %% "spark-mllib" % "2.1.0",
  "org.apache.hadoop" % "hadoop-client" % "2.8.0",
  "org.apache.spark" %% "spark-mllib-local" % "2.1.0",
  ws
)

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.1"
)

libraryDependencies ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.4",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.7.4"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"

libraryDependencies += filters

libraryDependencies += "com.google.inject" % "guice" % "3.0"

libraryDependencies += "com.amazonaws" % "aws-java-sdk-core" % "1.11.147"

libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.123"

libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.6.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0"

libraryDependencies += "org.codehaus.janino" % "janino" % "3.0.7"

libraryDependencies += "javax.inject" % "javax.inject" % "1"

libraryDependencies ++= Seq(
  "net.debasishg" %% "redisclient" % "3.4"
)

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.6"

javaOptions in Test += "-Dconfig.file=conf/application.test.conf"
