package zio.keeper.example

import zio.clock.Clock
import zio.console._
import zio.duration._
import zio.keeper.discovery.Discovery
import zio.keeper.membership.SWIM
import zio.keeper.transport
import zio.keeper.transport.Transport
import zio.macros.delegate._
import zio.nio.{ InetAddress, SocketAddress }
import zio.random.Random
import zio.{ Chunk, Schedule, ZIO, ZManaged }

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
  val withTransport = transport.tcp.withTcpTransport(10.seconds, 10.seconds)

  type Remainder = Console with Clock with Random

  private def environment(port: Int, others: Set[Int]) =
    for {
      transport <- (ZManaged.environment[Remainder] @@ withTransport)
      addr <- ZIO
               .foreach(others)(
                 port => InetAddress.localHost.flatMap(addr => SocketAddress.inetSocketAddress(addr, port))
               )
               .orDie
               .toManaged_
      config = Discovery.staticList(addr.toSet)
      env <- ZManaged
              .environment[Remainder]
              .flatMap(r => ZManaged.succeed(r) @@ enrichWith(config) @@ enrichWith(transport))
      swim <- (ZManaged.environment[Remainder with Discovery with Transport] @@ SWIM.withSWIM(port))
               .provide(env)
    } yield swim

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

  override def run(args: List[String]) =
    (for {
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

  override def run(args: List[String]) =
    (for {
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
