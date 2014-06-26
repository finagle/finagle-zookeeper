import sbt._
import Keys._

object finaglezk extends Build {
  val FinVersion = "6.17.0"

  logLevel := Level.Info

  val baseSettings = Defaults.defaultSettings ++ Seq(
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "1.9.2",
      "com.twitter" %% "finagle-core" % FinVersion,
      "junit" % "junit" % "4.11" % "test",
      "org.mockito" % "mockito-all" % "1.9.5" % "test",
      "org.slf4j" % "slf4j-api" % "1.7.7"
    ),
    resolvers ++= Seq("snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "releases" at "http://oss.sonatype.org/content/repositories/releases",
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "twitter-repo" at "http://maven.twttr.com"
    )
  )

  val publishSettings : Seq[sbt.Project.Setting[_]] = Seq(
    publishTo := Some("newzly releases" at "http://maven.newzly.com/repository/internal"),
    credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),
    publishMavenStyle := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => true }
  )

  lazy val buildSettings = Seq(
    organization := "com.twitter.finagle",
    version := FinVersion,
    crossScalaVersions := Seq("2.9.2", "2.10.0"),
    logLevel := Level.Debug
  )

  lazy val root = Project(id = "finagle-zk",
    base = file("."),
    settings = Defaults.itSettings ++ baseSettings ++ buildSettings ++ publishSettings)
    .configs(IntegrationTest)
}
