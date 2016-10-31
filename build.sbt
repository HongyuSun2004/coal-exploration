name := "aria"

organization := "com.owneriq.wh"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.0",
  "org.apache.spark" %% "spark-streaming" % "2.0.0",
  "org.apache.spark" %% "spark-sql" % "2.0.0",
  "com.couchbase.client" %% "spark-connector" % "2.0.0"
)