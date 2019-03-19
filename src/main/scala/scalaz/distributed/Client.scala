package scalaz.distributed

import java.net.InetAddress

trait Client {
  final def key[K: Type, V: Type](k: K): Path[Map[K, V], V] = Path.Key[K, V](k)

  final def compose[A: Type, B: Type, C: Type](x: Path[A, B], y: Path[B, C]): Path[A, C] =
    Path.Composed(x, y)

  def connect(member: Member, seed: Set[InetAddress]): F[Protocol]

  def members(cb: Membership => F[Boolean]): F[Unit]
}

object Client {

  def default: Client =
    new Client {
      override def connect(member: Member, seed: Set[InetAddress]): F[Protocol] = ???

      override def members(cb: Membership => F[Boolean]): F[Unit] = ???
    }
}
