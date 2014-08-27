import sbt._
import Keys._

object finaglezk extends Build {
  val finagleVersion = "6.20.0"
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
      "org.scalatest" %% "scalatest" % "2.2.0",
      "com.twitter" %% "finagle-core" % finagleVersion,
      "junit" % "junit" % "4.11",
      "com.google.guava" % "guava" % "17.0",
      "org.mockito" % "mockito-all" % "1.9.5" % "test"
    )
  )

  lazy val buildSettings = Seq(
    name := "finagle-ZooKeeper",
    organization := "com.twitter",
    version := clientVersion,
    crossScalaVersions := Seq("2.10.4"),
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")
  )

  lazy val runTests = taskKey[Unit]("Runs configurations and tests")
  lazy val testSettings = Seq(
    runTests := IntegrationTest.integrationTestTask(testOnly in Test).value,
    parallelExecution := false
  )
}