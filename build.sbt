ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.0"

lazy val root = (project in file("."))
  .settings(
    name := "ofac-parser"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql"  % "3.0.0"
libraryDependencies += "com.databricks"   %% "spark-xml"  % "0.15.0"