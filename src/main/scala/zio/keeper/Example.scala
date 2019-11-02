package zio.keeper

import java.util.concurrent.TimeUnit

import zio.console._
import zio.keeper.Cluster.Transport.TCPTransport
import zio.keeper.Cluster.{ Credentials, Discovery }
import zio.nio.{ InetAddress, SocketAddress }
import zio.{ Chunk, IO }

object Node1 extends zio.ManagedApp {

  val config = new Credentials
    with TCPTransport
    with Discovery
    with zio.console.Console.Live
    with zio.clock.Clock.Live
    with zio.random.Random.Live {

    override def discover: IO[Error, Set[SocketAddress]] =
      IO.succeed(Set.empty)
  }

  val appLogic = Cluster
    .join(5557)
    .provide(config)
    .flatMap(
      c =>
        //zio.ZIO.sleep(zio.duration.Duration(5, TimeUnit.SECONDS)) *>
        c.broadcast(Chunk.fromArray("foo".getBytes)).map(_ => c).toManaged_
    )
    .flatMap(
      c =>
        c.receive.foreach(
          n =>
            putStrLn(new String(n.payload.toArray))
              *> c.send(n.payload, n.sender)
              *> zio.ZIO.sleep(zio.duration.Duration(5, TimeUnit.SECONDS))
        ).toManaged_
    )

  def run(args: List[String]) =
    appLogic.fold(ex => {
      println(ex)
      1
    }, _ => 0)
}

object Node2 extends zio.ManagedApp {

  val config = new Credentials
    with TCPTransport
    with Discovery
    with zio.console.Console.Live
    with zio.clock.Clock.Live
    with zio.random.Random.Live {

    override def discover: IO[Error, Set[SocketAddress]] =
      InetAddress.localHost
        .flatMap(addr => SocketAddress.inetSocketAddress(addr, 5557))
        .map(Set(_: SocketAddress))
        .orDie
  }

  val appLogic = Cluster
    .join(5558)
    .provide(config)
    .flatMap(
      c =>
        //zio.ZIO.sleep(zio.duration.Duration(5, TimeUnit.SECONDS)) *>
        c.broadcast(Chunk.fromArray("bar".getBytes)).as(c).toManaged_
    )
    .flatMap(
      c =>
        c.receive.foreach(
          n =>
            putStrLn(new String(n.payload.toArray))
              *> c.send(n.payload, n.sender)
              *> zio.ZIO.sleep(zio.duration.Duration(5, TimeUnit.SECONDS))
        ).toManaged_
    )

  def run(args: List[String]) =
    appLogic.fold(_ => 1, _ => 0)

}

object Node3 extends zio.ManagedApp {

  val config = new Credentials
    with TCPTransport
    with Discovery
    with zio.console.Console.Live
    with zio.clock.Clock.Live
    with zio.random.Random.Live {

    override def discover: IO[Error, Set[SocketAddress]] =
      InetAddress.localHost
        .flatMap(addr => SocketAddress.inetSocketAddress(addr, 5558))
        .map(Set(_: SocketAddress))
        .orDie
  }

  val appLogic = Cluster
    .join(5559)
    .provide(config)
    .flatMap(
      c =>
        //zio.ZIO.sleep(zio.duration.Duration(5, TimeUnit.SECONDS)) *>
        c.broadcast(Chunk.fromArray("bar1".getBytes)).as(c).toManaged_
    )
    .flatMap(
      c =>
        c.receive.foreach(
          n =>
            putStrLn(new String(n.payload.toArray))
              *> c.send(n.payload, n.sender)
              *> zio.ZIO.sleep(zio.duration.Duration(5, TimeUnit.SECONDS))
        ).toManaged_
    )

  def run(args: List[String]) =
    appLogic.fold(_ => 1, _ => 0)

}
