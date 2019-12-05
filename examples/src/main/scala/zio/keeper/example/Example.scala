package zio.keeper.example

import java.util.concurrent.TimeUnit

import zio.clock.Clock
import zio.console._
import zio.duration._
import zio.keeper.discovery.Discovery
import zio.keeper.transport.tcp
import zio.keeper.{ Cluster, transport }
import zio.macros.delegate._
import zio.nio.{ InetAddress, SocketAddress }
import zio.random.Random
import zio.{ Chunk, Schedule, ZIO }

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

  private def environment(others: Set[Int]) =
    for {
      transport <- (ZIO.environment[Clock with Console with Random] @@ withTransport)
      addr <- ZIO
               .foreach(others)(
                 port => InetAddress.localHost.flatMap(addr => SocketAddress.inetSocketAddress(addr, port))
               )
               .orDie
      config = Discovery.staticList(addr.toSet)
      result <- ZIO.succeed(config) @@ enrichWith(transport)
    } yield result

  def start(port: Int, nodeName: String, otherPorts: Set[Int]) =
    environment(otherPorts).toManaged_ >>> Cluster
      .join(port)
      .flatMap(
        c =>
          (zio.ZIO.sleep(zio.duration.Duration(5, TimeUnit.SECONDS)) *>
            c.broadcast(Chunk.fromArray(nodeName.getBytes)).ignore.as(c)).toManaged_
      )
      .flatMap(
        c =>
          c.receive
            .foreach(
              n =>
                putStrLn(new String(n.payload.toArray))
                  *> c.send(n.payload, n.sender).ignore
                  *> zio.ZIO.sleep(zio.duration.Duration(5, TimeUnit.SECONDS))
            )
            .toManaged_
      )
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
