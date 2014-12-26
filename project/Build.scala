import sbt._
import Keys._

object finaglezk extends Build {

  val FinVersion = "6.24.0"

  val ScalatestVersion = "2.2.1"

  logLevel := Level.Info


  val baseSettings = Defaults.coreDefaultSettings ++ Seq(
    libraryDependencies ++= Seq(
      "org.scalatest"     %% "scalatest"      % ScalatestVersion,
      "com.twitter"       %% "finagle-core"   % FinVersion,
      "junit"              % "junit"          % "4.11" % "test",
      "org.mockito"        % "mockito-all"    % "1.9.5" % "test",
      "org.slf4j"          % "slf4j-api"      % "1.7.7"
    ),
    resolvers ++= Seq("snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "releases"            at "http://oss.sonatype.org/content/repositories/releases",
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "twitter-repo"        at "http://maven.twttr.com"
    )
  )

  val publishSettings : Seq[Def.Setting[_]] = Seq(
    publishTo := Some("newzly releases" at "http://maven.newzly.com/repository/internal"),
    credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),
    publishMavenStyle := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => true }
  )

  lazy val buildSettings = Seq(
    organization := "com.twitter",
    version := FinVersion,
    crossScalaVersions := Seq("2.10.4", "2.11.4"),
    logLevel := Level.Info
  )

  lazy val root = Project(
    id = "finagle-zookeeper",
    base = file("."),
    settings = Defaults.itSettings ++ baseSettings ++ buildSettings ++ publishSettings
  ).configs(IntegrationTest)
}
