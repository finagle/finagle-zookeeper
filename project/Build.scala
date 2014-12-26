import sbt._
import Keys._

object finaglezk extends Build {
  val finagleVersion = "6.24.0"
  val clientVersion = "0.2.0"

  lazy val root = Project(
    id = "finagle-ZooKeeper",
    base = file("."),
    settings = buildSettings
  ).aggregate(core, integration, example)

  lazy val core = Project(
    id = "core",
    base = file("core"),
    settings = baseSettings
  )

  lazy val example = Project(
    id = "example",
    base = file("example")
  ).dependsOn(core)

  lazy val integration = Project(
    id = "integration",
    base = file("integration"),
    settings = testSettings
  ).dependsOn(core, example)

  lazy val baseSettings = Seq(
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "2.2.2",
      "com.twitter" %% "finagle-core" % finagleVersion,
      "junit" % "junit" % "4.12",
      "org.mockito" % "mockito-all" % "1.10.8" % "test"
    )
  )

  lazy val buildSettings = Seq(
    name := "finagle-ZooKeeper",
    organization := "com.twitter",
    version := clientVersion,
    scalaVersion := "2.10.4",
    crossScalaVersions := Seq("2.10.4", "2.11.4"),
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")
  )

  lazy val runTests = taskKey[Unit]("Runs configurations and tests")
  lazy val testSettings = Seq(
    runTests := IntegrationTest.integrationTestTask(testOnly in Test).value,
    parallelExecution := false
  )
}
