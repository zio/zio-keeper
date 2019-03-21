package scalaz.distributed

import java.net.InetAddress

trait Client {
  type Type[A]
  type Path[A, B]

  trait Metadata {
    def get[A: Type, B: Type](where: Path[A, B]): F[B]
    def set[A: Type, B: Type](where: Path[A, B], b: B): F[Unit]

    final def modify[A: Type, B: Type](where: Path[A, B], f: B => B): F[Unit] =
      for {
        c <- get(where)
        r <- set(where, f(c))
      } yield r

    def increment[A: Type](where: Path[A, Int]): F[Unit]
  }

  trait Protocol {
    def access[A: Type](id: MetadataID): F[Metadata]
  }

  def key[K: Type, V: Type](k: K): Path[Map[K, V], V]

  def compose[A: Type, B: Type, C: Type](x: Path[A, B], y: Path[B, C]): Path[A, C]

  def connect(member: Member, seed: Set[InetAddress]): F[Protocol]

  def members(cb: Membership => F[Boolean]): F[Unit]
}

object Client {

  def default: Client =
    new Client {
      type Type[A]    = SupportedType[A]
      type Path[A, B] = PathElem[A, B]

      override def key[K: Type, V: Type](k: K): Path[Map[K, V], V] = PathElem.Key[K, V](k)

      override def compose[A: Type, B: Type, C: Type](x: Path[A, B], y: Path[B, C]): Path[A, C] =
        x >>> y

      override def connect(member: Member, seed: Set[InetAddress]): F[Protocol] = ???

      override def members(cb: Membership => F[Boolean]): F[Unit] = ???
    }
}
