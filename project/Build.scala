import sbt._
import Keys._

object finaglezk extends Build {
  val finagleVersion = "6.20.0"
  val clientVersion = "0.2.0"

  lazy val root = Project(
    id = "finagle-ZooKeeper",
    base = file("."),
    settings = buildSettings
  ).aggregate(core, integrationTesting)

  lazy val core = Project(
    id = "core",
    base = file("core"),
    settings = baseSettings
  )

  lazy val integrationTesting = Project(
    id = "integrationTesting",
    base = file("integrationTesting"),
    settings = testSettings
  ).dependsOn(core)

  lazy val baseSettings = libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "2.2.0",
    "com.twitter" %% "finagle-core" % finagleVersion,
    "junit" % "junit" % "4.11",
    "org.mockito" % "mockito-all" % "1.9.5" % "test"
  )

  lazy val buildSettings = Seq(
    name := "finagle-ZooKeeper",
    organization := "com.twitter.finagle",
    version := clientVersion,
    crossScalaVersions := Seq("2.9.2", "2.10.4")
  )

  lazy val runTests = taskKey[Unit]("Runs configurations and tests")
  lazy val testSettings = Seq(
    runTests := IntegrationTest.integrationTestTask(testOnly in Test).value,
    parallelExecution in runTests := false,
    parallelExecution in Test := false
  )
}