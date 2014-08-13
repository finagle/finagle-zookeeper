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
      "/integrationTesting/src/main/resources/testConfig.csv"
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
          + "/integrationTesting/src/main/resources/scripts/runQuorumMode.sh "
          + s"$version $URL"
          ).!!

      case "standalone" =>
        ("sh "
          + System.getProperty("user.dir")
          + "/integrationTesting/src/main/resources/scripts/runStandaloneMode.sh "
          + s"$version $URL"
          ).!!
    }
  }

  def cleanEnv(mode: String, version: String): Unit = {
    mode match {
      case "quorum" =>
        ("sh "
          + System.getProperty("user.dir")
          + "/integrationTesting/src/main/resources/scripts/stopAndCleanQuorumMode.sh "
          + version
          ).!!

      case "standalone" =>
        ("sh "
          + System.getProperty("user.dir")
          + "/integrationTesting/src/main/resources/scripts/stopAndCleanStandaloneMode.sh "
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

  def sequence(tasks: Initialize[Task[Unit]]*): Initialize[Task[Unit]] = {
    tasks.reverse.reduceLeft { (a, b) =>
      val combinedTask: Initialize[Task[Unit]] = {
        a.zip(b).apply { case (task1, task2) =>
          task1.dependsOn(task2)
        }
      }
      combinedTask
    }
  }

  def integrationTestTask(
    testRunner: InputKey[Unit]
  ): Def.Initialize[Task[Unit]] = Def.taskDyn {
    val tasks = fileParser.value flatMap { config =>
      config.modes map { mode =>
        sequence(
          configEnv.toTask(s" ${ config.version } $mode"),
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