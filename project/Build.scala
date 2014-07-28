import sbt._
import Keys._

object finaglezk extends Build {
  val FinVersion = "6.18.0"

  logLevel := Level.Info

  val baseSettings = Defaults.defaultSettings ++ Seq(
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "1.9.2",
      "com.twitter" %% "finagle-core" % FinVersion,
      "junit" % "junit" % "4.11" % "test",
      "org.mockito" % "mockito-all" % "1.9.5" % "test",
      "org.slf4j" % "slf4j-api" % "1.7.7"
    )
  )

  lazy val buildSettings = Seq(
    organization := "com.twitter.finagle",
    version := FinVersion,
    crossScalaVersions := Seq("2.9.2", "2.10.4"),
    logLevel := Level.Debug,
    parallelExecution := false
  )

  lazy val root = Project(id = "finagle-zk",
    base = file("."),
    settings = Defaults.itSettings ++ baseSettings ++ buildSettings)
    .configs(IntegrationTest)
}
