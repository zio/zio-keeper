package zio.keeper

import java.net.InetAddress

import zio.{ IO, ZIO }
import zio.stream.Stream
import zio.keeper.Cluster._

trait Cluster {
  final def join: ZIO[Credentials with Discovery with Transport, Error, Unit] = ???
}

object Cluster {
  trait Credentials {
    // TODO: ways to obtain auth data
  }

  trait Discovery {
    def discover: IO[Error, Set[InetAddress]]
  }

  trait Gossip {
    // TODO: gossipping interface
  }

  trait Transport {
    def send[S, A](data: Message[S, A]): IO[Error, Unit]
    def receive[S, A]: Stream[Error, Message[S, A]]
  }
}
