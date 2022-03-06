ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.8"

lazy val root = (project in file("."))
  .settings(
    name := "truata_assignment",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.3",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.3",
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.3",
    libraryDependencies += "org.scalatest" %% "scalatest-funspec" % "3.2.9" % Test
  )
