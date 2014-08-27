import sbt._
import Def.Initialize
import complete.DefaultParsers._

object IntegrationTest {
  case class Config(
    version: String,
    modes: Seq[String],
    packages: Seq[String]
  )

  /**
   * Parse the testConfig.csv file. Line example :
   * 3.5.0;standalone quorum;v3_4 v3_5
   */
  lazy val fileParser: Initialize[Task[Seq[Config]]] = Def.task {
    val path = System.getProperty("user.dir") +
      "/integration/src/main/resources/testConfig.csv"
    val source = scala.io.Source.fromFile(path)
    val configs = source.getLines().toSeq map { line =>
      val Array(version, modes, packages) = line.split(";")
      Config(
        version,
        modes.split(" "),
        packages.split(" ")
      )
    }

    configs
  }

  /**
   * Calls the shell script in charge of configuring the server/quorum
   * @return Unit
   */
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

  /**
   * Check if the server or quorum is available by sending the four letter command.
   * Consists of executing `echo mntr | nc 127.0.0.1 2181` and parsing the response.
   * If we are in standalone mode, it will check that the response contains "standalone".
   * If we are in quorum mode, it will check that we have one "leader" and two "follower".
   *
   * @return Unit
   */
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

    def sendMntrCommand(ip: String, port: String): String = {
      try { ("echo mntr" #| s"nc $ip $port").!! }
      catch {
        case _: Throwable =>
          Thread.sleep(2000)
          sendMntrCommand(ip, port)
      }
    }

    def isServerAvailable: Boolean = {
      val rep = sendMntrCommand("127.0.0.1", "2181")
      rep.contains("standalone")
    }

    def isQuorumAvailable: Boolean = {
      def evalServer(ip: String, port: String): Int = {
        val rep = sendMntrCommand(ip, port)
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

    recursiveCheckEnv(mode)
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
    packages: Seq[String]
  ): Initialize[Task[Unit]] = {
    val packagesTasks = packages map { packge =>
      testRunner.toTask(
        s" com.twitter.finagle.exp.zookeeper.integration.$mode.$packge.*"
      )
    }

    sequence(packagesTasks: _*)
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
          runTests(testRunner, mode, config.packages) andFinally {
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