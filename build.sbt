name := "Scala app"

version := "0.1.0"

scalaVersion := "2.11.0"

sbtVersion := "0.13.2"

organization := "com.pag"

libraryDependencies ++= {
  Seq(
    "org.scala-lang" % "scala-compiler" % "2.11.0",
    "org.scalatest" % "scalatest_2.11" % "2.1.4"
  )
}

mainClass := Some("com.pag.App")

resolvers ++= Seq("snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases" at "http://oss.sonatype.org/content/repositories/releases",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
)

scalacOptions ++= Seq("-unchecked", "-deprecation")

logLevel := Level.Info
