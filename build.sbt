ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "sparky"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1"

// for postgres
libraryDependencies += "org.postgresql" % "postgresql" % "42.5.1"

// for minio
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.3.2"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.3.2"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.2"
