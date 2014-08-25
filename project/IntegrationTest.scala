import sbt._
import Def.Initialize
import complete.DefaultParsers._

object IntegrationTest {
  case class Config(
    version: String,
    modes: Seq[String],
    commands: Seq[String],
    extras: Seq[String]
  )

  lazy val fileParser: Initialize[Task[Seq[Config]]] = Def.task {
    val path = System.getProperty("user.dir") +
      "/integration/src/main/resources/testConfig.csv"
    val source = scala.io.Source.fromFile(path)
    val configs = source.getLines().toSeq map { line =>
      val Array(version, modes, commands, extras) = line.split(";")
      Config(
        version,
        modes.split(" "),
        commands.split(" "),
        extras.split(" ")
      )
    }

    configs
  }

  def configEnv: Initialize[InputTask[Unit]] = Def.inputTask {
    val args: Seq[String] = spaceDelimited("<arg>").parsed

    val Seq(version, mode) = args
    val baseURL = "https://github.com/apache/zookeeper/archive/release-"
    val URL = baseURL + version + ".tar.gz"

    mode match {
      case "quorum" =>
        ("sh "
          + System.getProperty("user.dir")
          + "/integration/src/main/resources/scripts/runQuorumMode.sh "
          + s"$version $URL"
          ).!!

      case "standalone" =>
        ("sh "
          + System.getProperty("user.dir")
          + "/integration/src/main/resources/scripts/runStandaloneMode.sh "
          + s"$version $URL"
          ).!!
    }
  }

  def checkEnv: Initialize[InputTask[Unit]] = Def.inputTask {
    val args: Seq[String] = spaceDelimited("<arg>").parsed
    val Seq(mode) = args

    def recursiveCheckEnv(mode: String): Unit = {
      mode match {
        case "quorum" =>
          if (isQuorumAvailable) Unit
          else {
            Thread.sleep(2000)
            recursiveCheckEnv(mode)
          }

        case "standalone" =>
          if (isServerAvailable) Unit
          else {
            Thread.sleep(2000)
            recursiveCheckEnv(mode)
          }
      }
    }

    recursiveCheckEnv(mode)
  }

  def isServerAvailable: Boolean = {
    val rep = ("echo mntr" #| "nc 127.0.0.1 2181" !!)
    rep.contains("standalone")
  }

  def isQuorumAvailable: Boolean = {
    def evalServer(ip: String, port: String): Int = {
      val rep = ("echo mntr" #| s"nc $ip $port" !!)
      if (rep.contains("leader")) 2
      else if (rep.contains("follower")) 1
      else 0
    }

    val total = Seq(
      ("127.0.0.1", "2181"),
      ("127.0.0.1", "2182"),
      ("127.0.0.1", "2183")
    ).foldLeft(0) { (acc, ad) => acc + evalServer(ad._1, ad._2) }

    if (total == 4) true
    else false
  }

  def cleanEnv(mode: String, version: String): Unit = {
    mode match {
      case "quorum" =>
        ("sh "
          + System.getProperty("user.dir")
          + "/integration/src/main/resources/scripts/stopAndCleanQuorumMode.sh "
          + version
          ).!!

      case "standalone" =>
        ("sh "
          + System.getProperty("user.dir")
          + "/integration/src/main/resources/scripts/stopAndCleanStandaloneMode.sh "
          + version
          ).!!
    }
  }

  def runTests(
    testRunner: InputKey[Unit],
    mode: String,
    commands: Seq[String],
    extras: Seq[String]
  ): Initialize[Task[Unit]] = {
    val commandsTasks = commands map { command =>
      testRunner.toTask(
        s" com.twitter.finagle.exp.zookeeper.integration.$mode.command.$command"
      )
    }
    val extrasTasks = extras map { extra =>
      testRunner.toTask(
        s" com.twitter.finagle.exp.zookeeper.integration.$mode.extra.$extra"
      )
    }

    sequence(commandsTasks ++ extrasTasks: _*)
  }

  def sequence(tasks: Initialize[Task[Unit]]*): Initialize[Task[Unit]] =
    tasks match {
      case Seq() => Def.task { () }
      case Seq(x, xs@_*) => Def.taskDyn { val _ = x.value; sequence(xs: _*) }
    }

  def integrationTestTask(
    testRunner: InputKey[Unit]
  ): Def.Initialize[Task[Unit]] = Def.taskDyn {
    val tasks = fileParser.value flatMap { config =>
      config.modes map { mode =>
        sequence(
          configEnv.toTask(s" ${ config.version } $mode"),
          checkEnv.toTask(s" $mode"),
          runTests(testRunner, mode, config.commands, config.extras) andFinally {
            cleanEnv(mode, config.version)
          }
        )
      }
    }

    Def.task {
      sequence(tasks: _*).value
    }
  }
}