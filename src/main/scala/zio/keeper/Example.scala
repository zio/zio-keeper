package zio.keeper

import java.util.concurrent.TimeUnit

import zio.console._
import zio.keeper.Cluster.Transport.TCPTransport
import zio.keeper.Cluster.{ Credentials, Discovery }
import zio.nio.{ InetAddress, SocketAddress }
import zio.{ Chunk, IO }

object Node1 extends zio.App {

  val config = new Credentials with TCPTransport with Discovery with zio.console.Console.Live with zio.clock.Clock.Live
  with zio.random.Random.Live {

    override def discover: IO[Error, Set[SocketAddress]] =
      InetAddress.localHost
        .flatMap(
          addr =>
            zio.ZIO
              .collectAll(
                List(
                  SocketAddress.inetSocketAddress(addr, 5557),
                  SocketAddress.inetSocketAddress(addr, 5558)
                )
              )
              .map(_.toSet[SocketAddress])
        )
        .orDie
  }

  val appLogic = Cluster
    .join(5557)
    .provide(config)
    .flatMap(
      c =>
        //zio.ZIO.sleep(zio.duration.Duration(5, TimeUnit.SECONDS)) *>
        c.broadcast(Chunk.fromArray("foo".getBytes)).map(_ => c)
    )
    .flatMap(
      c =>
        c.receive.foreach(
          n =>
            putStrLn(new String(n.payload.toArray))
              *> c.send(n.payload, n.sender)
              *> zio.ZIO.sleep(zio.duration.Duration(5, TimeUnit.SECONDS))
        )
    )

  def run(args: List[String]) =
    appLogic.fold(ex => {
      println(ex)
      1
    }, _ => 0)
}

object Node2 extends zio.App {

  val config = new Credentials with TCPTransport with Discovery with zio.console.Console.Live with zio.clock.Clock.Live
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
        c.broadcast(Chunk.fromArray("bar".getBytes)).map(_ => c)
    )
    .flatMap(
      c =>
        c.receive.foreach(
          n =>
            putStrLn(new String(n.payload.toArray))
              *> c.send(n.payload, n.sender)
              *> zio.ZIO.sleep(zio.duration.Duration(5, TimeUnit.SECONDS))
        )
    )

  def run(args: List[String]) =
    appLogic.fold(_ => 1, _ => 0)

}

object Node3 extends zio.App {

  val config = new Credentials with TCPTransport with Discovery with zio.console.Console.Live with zio.clock.Clock.Live
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
        c.broadcast(Chunk.fromArray("bar1".getBytes)).map(_ => c)
    )
    .flatMap(
      c =>
        c.receive.foreach(
          n =>
            putStrLn(new String(n.payload.toArray))
              *> c.send(n.payload, n.sender)
              *> zio.ZIO.sleep(zio.duration.Duration(5, TimeUnit.SECONDS))
        )
    )

  def run(args: List[String]) =
    appLogic.fold(_ => 1, _ => 0)

}
