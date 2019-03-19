package scalaz.distributed

import java.net.InetAddress

trait Module {
  final def key[K: Type, V: Type](k: K): Path[Map[K, V], V] = Path.Key[K, V](k)

  final def compose[A: Type, B: Type, C: Type](x: Path[A, B], y: Path[B, C]): Path[A, C] =
    Path.Composed(x, y)

  def members(cb: Membership => F[Boolean]): F[Unit]

  def startup(member: Member, seed: Set[InetAddress]): F[Protocol]
}
