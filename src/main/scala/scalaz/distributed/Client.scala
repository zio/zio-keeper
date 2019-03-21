package scalaz.distributed

import java.net.InetAddress

import scalaz.zio.IO

trait Client {
  type Distributed[A] = IO[Error, A]

  type Type[A]

  trait Metadata {
    def get[A: Type, B: Type](where: Path[A, B]): Distributed[B]
    def set[A: Type, B: Type](where: Path[A, B], b: B): Distributed[Unit]
  }

  trait Protocol {
    def access[A: Type](id: MetadataID): Distributed[Metadata]
  }

  def key[K: Type, V: Type](k: K): Path[Map[K, V], V]

  def compose[A: Type, B: Type, C: Type](x: Path[A, B], y: Path[B, C]): Path[A, C]

  def connect(member: Member, seed: Set[InetAddress]): Distributed[Protocol]

  def members(cb: Membership => Distributed[Boolean]): Distributed[Unit]
}
