package zio.keeper

import java.net.InetAddress

import zio.{ IO, ZIO }
import zio.stream.Stream
import zio.keeper.Cluster._

trait Cluster {
  final def join: ZIO[Credentials with Discovery with Transport with Metadata, Error, Handle] = ???
}

object Cluster {
  // TODO: properly represent (see current 'develop' branch)
  type Path[_, _]
  type Type[_]

  object Type {
    def apply[A](implicit instance: Type[A]): Type[A] = instance
  }

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

  // TODO: maybe just an enumeration specifying the type of underlying data structure, e.g. CRDT
  trait Metadata {}

  trait Handle {
    def get[A: Type, B: Type](where: Path[A, B]): IO[Error, B]
    def set[A: Type, B: Type](where: Path[A, B], b: B): IO[Error, Unit]
  }
}
