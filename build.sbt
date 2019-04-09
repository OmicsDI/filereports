name := "filereprots"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.1"
)