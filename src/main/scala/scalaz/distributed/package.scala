package scalaz

import java.net.InetAddress

import scalaz.zio.IO

package object distributed {
  type Distributed[A] = IO[Error, A]

  val client = new DistributedModule {
    type Type[A] = SupportedType[A]

    override def key[K: Type, V: Type](k: K): Path[Map[K, V], V] = Path.Key[K, V](k)

    override def compose[A: Type, B: Type, C: Type](x: Path[A, B], y: Path[B, C]): Path[A, C] =
      x >>> y

    override def connect(member: Member, seed: Set[InetAddress]): Distributed[Protocol] = ???

    override def members(cb: Membership => Distributed[Boolean]): Distributed[Unit] = ???
  }
}
