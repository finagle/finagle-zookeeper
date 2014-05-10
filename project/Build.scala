import sbt._
import Keys._

object finaglezk extends Build {
  val FinVersion = "6.15.0"

  val baseSettings = Defaults.defaultSettings ++ Seq(
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "1.9.2",
      "com.twitter" %% "finagle-core" % FinVersion,
      "com.twitter" %% "util-core" % FinVersion,
      "junit" % "junit" % "4.11" % "test",
      "org.mockito" % "mockito-all" % "1.9.5" % "test"
    ),
    resolvers ++= Seq("snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "releases" at "http://oss.sonatype.org/content/repositories/releases",
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "twitter-repo" at "http://maven.twttr.com"
    )
  )

  lazy val buildSettings = Seq(
    organization := "com.twitter.finagle",
    version := FinVersion,
    crossScalaVersions := Seq("2.9.2", "2.10.0"),
    scalaVersion := "2.9.2"
  )

  lazy val root = Project(id = "finagle-zk",
    base = file("."),
    settings = Defaults.itSettings ++ baseSettings ++ buildSettings)
    .configs(IntegrationTest)
}