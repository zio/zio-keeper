package zio.keeper.example

import zio.clock.Clock
import zio.console._
import zio.duration._
import zio.keeper.discovery.Discovery
import zio.keeper.membership.SWIM
import zio.keeper.transport
import zio.keeper.transport.Transport
import zio.logging.AbstractLogging
import zio.logging.slf4j.{Logging, LoggingFormat, Slf4jLogger}
import zio.macros.delegate._
import zio.nio.{InetAddress, SocketAddress}
import zio.random.Random
import zio.{Chunk, Schedule, ZIO, ZManaged}

object Node1 extends zio.ManagedApp {

  def run(args: List[String]) =
    TestNode.start(5557, "Node1", Set.empty)
}

object Node2 extends zio.ManagedApp {

  def run(args: List[String]) =
    TestNode.start(5558, "Node2", Set(5557))
}

object Node3 extends zio.ManagedApp {

  def run(args: List[String]) =
    TestNode.start(5559, "Node3", Set(5558))
}

object TestNode {
  type Remainder = Console with Clock with Random

  val loggingEnv = ZIO.environment[zio.ZEnv] @@ enrichWith[Logging](
    new Logging {

      override def logging: AbstractLogging.Service[Any, String] = new Slf4jLogger {

        override val slf4jMessageFormat: LoggingFormat = new LoggingFormat {

          override def format(message: String): ZIO[Any, Nothing, String] =
            ZIO.succeed(message)
        }
      }
    }
  )

  def discoveryEnv(others: Set[Int]) = ZIO.environment[zio.ZEnv] @@
    enrichWithM[Discovery](
    ZIO
      .foreach(others)(
        port => InetAddress.localHost.flatMap(addr => SocketAddress.inetSocketAddress(addr, port))
      )
      .orDie
        .map(addrs => Discovery.staticList(addrs.toSet))
  )


  def membership(port: Int) =
    ZManaged.environment[zio.ZEnv with Logging with Transport with Discovery] @@ enrichWithManaged(
      SWIM.join(port)
    )

  val tcp =
    loggingEnv >>> ZIO.environment[zio.ZEnv with Logging] @@ transport.tcp.withTcpTransport(10.seconds, 10.seconds)

  private def environment(port: Int, others: Set[Int]) =
    (discoveryEnv(others).toManaged_ @@
      enrichWithManaged(loggingEnv.toManaged_) @@
      enrichWithManaged(tcp.toManaged_)) >>>
      membership(port)


  import zio.clock._
  import zio.keeper.membership._

  def start(port: Int, nodeName: String, otherPorts: Set[Int]) =
    (environment(port, otherPorts) >>> (for {
      _ <- sleep(5.seconds).toManaged_
      _ <- broadcast(Chunk.fromArray(nodeName.getBytes)).ignore.toManaged_
      _ <- receive
            .foreach(
              message =>
                putStrLn(new String(message.payload.toArray))
                  *> send(message.payload, message.sender).ignore
                  *> sleep(5.seconds)
            )
            .toManaged_
    } yield ()))
      .fold(ex => {
        println(s"exit with error: $ex")
        1
      }, _ => 0)
}

object TcpServer extends zio.App {
  import zio._
  import zio.duration._
  import zio.keeper.transport._

  val localEnvironment = ZIO.environment[zio.ZEnv] @@ enrichWith[Logging](
    new Logging {

      override def logging: AbstractLogging.Service[Any, String] = new Slf4jLogger {

        override val slf4jMessageFormat: LoggingFormat = new LoggingFormat {

          override def format(message: String): ZIO[Any, Nothing, String] =
            ZIO.succeed(message)
        }
      }
    }
  )

  override def run(args: List[String]) =
    localEnvironment >>> (for {
      tcp       <- tcp.tcpTransport(10.seconds, 10.seconds)
      localHost <- InetAddress.localHost.orDie
      publicAddress <- SocketAddress
                        .inetSocketAddress(localHost, 8010)
                        .orDie
      console <- ZIO.environment[Console]
      handler = (channel: ChannelOut) => {
        for {
          data <- channel.read
          _    <- putStrLn(new String(data.toArray))
          _    <- channel.send(data)
        } yield ()
      }.forever
        .catchAll(ex => putStrLn("error: " + ex.msg))
        .provide(console)

      _ <- putStrLn("public address: " + publicAddress.toString())
      _ <- bind(publicAddress)(handler)
            .provide(tcp)
            .use(ch => ZIO.never.ensuring(ch.close.ignore))
            .fork

    } yield ()).ignore.as(0)
}

object TcpClient extends zio.App {
  import zio.duration._
  import zio.keeper.transport._

  val localEnvironment = ZIO.environment[zio.ZEnv] @@ enrichWith[Logging](
    new Logging {

      override def logging: AbstractLogging.Service[Any, String] = new Slf4jLogger {

        override val slf4jMessageFormat: LoggingFormat = (message: String) => ZIO.succeed(message)
      }
    }
  )

  override def run(args: List[String]) =
    localEnvironment >>> (for {
      tcp       <- tcp.tcpTransport(10.seconds, 10.seconds)
      localHost <- InetAddress.localHost.orDie
      publicAddress <- SocketAddress
                        .inetSocketAddress(localHost, 8010)
                        .orDie
      _ <- putStrLn("connect to address: " + publicAddress.toString())
      _ <- connect(publicAddress)
            .provide(tcp)
            .use(_.send(Chunk.fromArray("message from client".getBytes)).repeat(Schedule.recurs(100)))
    } yield ()).ignore.as(0)
}
