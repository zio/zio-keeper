package zio.keeper.example

import java.util.concurrent.TimeUnit

import zio.clock.Clock
import zio.console._
import zio.duration._
import zio.keeper.Cluster.Credentials
import zio.keeper.discovery.Discovery
import zio.keeper.{ Cluster, Error, transport }
import zio.macros.delegate._
import zio.nio.{ InetAddress, SocketAddress }
import zio.random.Random
import zio.{ Chunk, IO, Schedule, ZIO }

object Node1 extends zio.ManagedApp {

  val withTransport = transport.tcp.withTcpTransport(10.seconds, 10.seconds)

  val env =
    for {
      transport <- (ZIO.environment[Clock with Console with Random] @@ withTransport)
      config: Credentials with Discovery = new Credentials with Discovery {

        override val discover: IO[Error, Set[SocketAddress]] =
          IO.succeed(Set.empty)
      }
      result <- ZIO.succeed(config) @@ enrichWith(transport)
    } yield result

  val appLogic = Cluster
    .join(5557)
    .flatMap(
      c =>
        (zio.ZIO.sleep(zio.duration.Duration(5, TimeUnit.SECONDS)) *>
          c.broadcast(Chunk.fromArray("foo".getBytes)).map(_ => c)).toManaged_
    )
    .flatMap(
      c =>
        c.receive
          .foreach(
            n =>
              putStrLn(new String(n.payload.toArray))
                *> c.send(n.payload, n.sender)
                *> zio.ZIO.sleep(zio.duration.Duration(5, TimeUnit.SECONDS))
          )
          .toManaged_
    )

  def run(args: List[String]) =
    env.toManaged_
      .flatMap(e => appLogic.provide(e))
      .fold(ex => {
        println(ex)
        1
      }, _ => 0)

}

object Node2 extends zio.ManagedApp {

  val withTransport = transport.tcp.withTcpTransport(10.seconds, 10.seconds)

  val env =
    for {
      transport <- (ZIO.environment[Clock with Console with Random] @@ withTransport)
      config: Credentials with Discovery = new Credentials with Discovery {

        override val discover: IO[Error, Set[SocketAddress]] =
          InetAddress.localHost
            .flatMap(addr => SocketAddress.inetSocketAddress(addr, 5557))
            .map(Set(_: SocketAddress))
            .orDie
      }
      result <- ZIO.succeed(config) @@ enrichWith(transport)
    } yield result

  val appLogic = Cluster
    .join(5558)
    .flatMap(
      c =>
        (zio.ZIO.sleep(zio.duration.Duration(5, TimeUnit.SECONDS)) *>
          c.broadcast(Chunk.fromArray("bar".getBytes)).as(c)).toManaged_
    )
    .flatMap(
      c =>
        c.receive
          .foreach(
            n =>
              putStrLn(new String(n.payload.toArray))
                *> c.send(n.payload, n.sender)
                *> zio.ZIO.sleep(zio.duration.Duration(5, TimeUnit.SECONDS))
          )
          .toManaged_
    )

  def run(args: List[String]) =
    env.toManaged_
      .flatMap(e => appLogic.provide(e))
      .fold(ex => {
        println(ex)
        1
      }, _ => 0)
}

object Node3 extends zio.ManagedApp {

  val withTransport = transport.tcp.withTcpTransport(10.seconds, 10.seconds)

  val env =
    for {
      transport <- (ZIO.environment[Clock with Console with Random] @@ withTransport)
      config: Credentials with Discovery = new Credentials with Discovery {

        override val discover: IO[Error, Set[SocketAddress]] =
          InetAddress.localHost
            .flatMap(addr => SocketAddress.inetSocketAddress(addr, 5558))
            .map(Set(_: SocketAddress))
            .orDie
      }
      result <- ZIO.succeed(config) @@ enrichWith(transport)
    } yield result

  val appLogic = Cluster
    .join(5559)
    .flatMap(
      c =>
        (zio.ZIO.sleep(zio.duration.Duration(5, TimeUnit.SECONDS)) *>
          c.broadcast(Chunk.fromArray("bar1".getBytes)).as(c)).toManaged_
    )
    .flatMap(
      c =>
        c.receive
          .foreach(
            n =>
              putStrLn(new String(n.payload.toArray))
                *> c.send(n.payload, n.sender)
                *> zio.ZIO.sleep(zio.duration.Duration(5, TimeUnit.SECONDS))
          )
          .toManaged_
    )

  def run(args: List[String]) =
    env.toManaged_
      .flatMap(e => appLogic.provide(e))
      .fold(ex => {
        println(ex)
        1
      }, _ => 0)

}

object Server extends zio.App {
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
      //TODO useForever caused dead code so we should find other way to block this from exit.
      _ <- bind(publicAddress)(handler)
            .provide(tcp)
            .useForever
            .fork

    } yield ()).as(0)
}

object Client extends zio.App {
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
