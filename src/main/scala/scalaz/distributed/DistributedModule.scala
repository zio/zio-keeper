package scalaz.distributed

import java.net.InetAddress

trait DistributedModule {
  def key[K: Type, V: Type](k: K): Path[Map[K, V], V]

  def compose[A: Type, B: Type, C: Type](x: Path[A, B], y: Path[B, C]): Path[A, C]

  def connect(member: Member, seed: Set[InetAddress]): Distributed[Protocol]

  def members(cb: Membership => Distributed[Boolean]): Distributed[Unit]
}
