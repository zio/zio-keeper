package zio.keeper.example

import zio._
import zio.clock._
import zio.console._
import zio.duration._
import zio.keeper.discovery.Discovery
import zio.keeper.discovery.static.Static
import zio.keeper.membership._
import zio.keeper.membership.swim.SWIM
import zio.keeper.transport.Channel.Connection
import zio.keeper.transport.tcp.Tcp
import zio.logging.Logging
import zio.nio.core.{ InetAddress, SocketAddress }
import zio.random.Random

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
  val logging = Logging.console((_, msg) => msg)

  val transport = (Clock.live ++ logging) >>> Tcp.live(10.seconds, 10.seconds)

  def start(port: Int, nodeName: String, otherPorts: Set[Int]) =
    environment(port, otherPorts).orDie
      .flatMap { env =>
        (for {
          _ <- sleep(5.seconds).toManaged_
          _ <- broadcast(Chunk.fromArray(nodeName.getBytes)).ignore.toManaged_
          _ <- receive
                .foreach(
                  message =>
                    putStrLn(s"id: ${message.id}, payload: ${new String(message.payload.toArray)}")
                      *> send(message.payload, message.sender).ignore
                      *> sleep(5.seconds)
                )
                .toManaged_
        } yield ()).provideCustomLayer(env)
      }
      .foldM(ex => putStrLn(s"exit with error: $ex").toManaged_.as(1), _ => Managed.succeed(0))

  private def environment(port: Int, others: Set[Int]) =
    discovery(others).map { dsc =>
      val mem = (dsc ++ transport ++ logging ++ Clock.live ++ Random.live) >>> membership(port)
      dsc ++ transport ++ logging ++ mem
    }

  def discovery(others: Set[Int]): Managed[Exception, Layer[Nothing, Discovery]] =
    ZManaged
      .foreach(others) { port =>
        InetAddress.localHost.flatMap(SocketAddress.inetSocketAddress(_, port)).toManaged_
      }
      .orDie
      .map(addrs => Static.live(addrs.toSet))

  def membership(port: Int) =
    SWIM.live(port)
}

object TcpServer extends zio.App {
  import zio._
  import zio.duration._
  import zio.keeper.transport._

  val logging = Logging.console((_, msg) => msg)

  val transport = (Clock.live ++ logging) >>> Tcp.live(10.seconds, 10.seconds)

  val localEnvironment = Console.live ++ transport

  override def run(args: List[String]) =
    (for {
      localHost <- InetAddress.localHost.orDie
      publicAddress <- SocketAddress
                        .inetSocketAddress(localHost, 8010)
                        .orDie
      console <- ZIO.environment[Console]
      handler = (channel: Connection) => {
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
            .use(ch => ZIO.never.ensuring(ch.close.ignore))
            .fork

    } yield ()).ignore.as(0).provideLayer(localEnvironment)
}

object TcpClient extends zio.App {
  import zio.keeper.transport._

  val logging = Logging.console((_, msg) => msg)

  val transport = (Clock.live ++ logging) >>> Tcp.live(10.seconds, 10.seconds)

  val localEnvironment = Console.live ++ transport

  override def run(args: List[String]) =
    (for {
      localHost <- InetAddress.localHost.orDie
      publicAddress <- SocketAddress
                        .inetSocketAddress(localHost, 8010)
                        .orDie
      _ <- putStrLn("connect to address: " + publicAddress.toString())
      _ <- connect(publicAddress)
            .use(_.send(Chunk.fromArray("message from client".getBytes)).repeat(Schedule.recurs(100)))
    } yield ()).ignore.as(0).provideLayer(localEnvironment)
}
