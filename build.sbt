name := """spark-data-movable"""

lazy val root = (project in file("."))

version := "0.1"

scalaVersion := "2.12.10"

Compile / run / scalacOptions ++= Seq("-deprecation")

val typesafeConfigVersion = "1.4.0"

val scalaTestVersion = "3.1.1"

val awsVersion = "1.11.971"

val sparkVersion = "3.1.1"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % typesafeConfigVersion,
  "org.slf4j" % "slf4j-api" % "1.7.30",
  "org.slf4j" % "slf4j-simple" % "1.7.30",

  "com.amazonaws" % "aws-java-sdk-s3" % awsVersion,
  "com.amazonaws" % "aws-java-sdk" % awsVersion,
  "com.amazonaws" % "aws-java-sdk-core" % awsVersion,


  "org.apache.hadoop" % "hadoop-common" % "3.3.0",
  "org.apache.hadoop" % "hadoop-client" % "3.3.0",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.0",


  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // jdbc connector with quill
  "org.postgresql" % "postgresql" % "42.2.8",
  "mysql" % "mysql-connector-java" % "8.0.17",
  "com.microsoft.sqlserver" % "mssql-jdbc" % "7.4.1.jre8",
  "com.h2database" % "h2" % "1.4.199",
  "io.getquill" %% "quill-jdbc" % "3.5.1",

  // test
  "org.scalactic" %% "scalactic" % scalaTestVersion,
  "org.scalamock" %% "scalamock" % "4.4.0" % Test,
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
  "com.h2database" % "h2" % "1.4.199" % Test

)
