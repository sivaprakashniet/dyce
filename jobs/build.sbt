name := """simple-jobs"""

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.apache.spark" %% "spark-mllib" % "2.1.0",
  "org.apache.hadoop" % "hadoop-client" % "2.8.0",
  "org.apache.spark" %% "spark-mllib-local" % "2.1.0"
)

libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.1.0"

libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.5.0"

libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.2"

libraryDependencies += "com.github.seratch" %% "awscala" % "0.6.+"

libraryDependencies += "com.typesafe.play" % "play-json_2.11" % "2.5.15"

resolvers += "OSS Sonatype" at "https://repo1.maven.org/maven2/"

libraryDependencies ++= Seq(
  "org.reactivemongo" %% "play2-reactivemongo" % "0.11.14"
)
