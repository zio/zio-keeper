package scalaz.distributed

import java.net.InetAddress

trait Client {
  type Type[A]
  type Path[A, B]

  def key[K: Type, V: Type](k: K): Path[Map[K, V], V]

  def compose[A: Type, B: Type, C: Type](x: Path[A, B], y: Path[B, C]): Path[A, C]

  def connect(member: Member, seed: Set[InetAddress]): F[Protocol[Type, Path]]

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

      override def connect(member: Member, seed: Set[InetAddress]): F[Protocol[Type, Path]] = ???

      override def members(cb: Membership => F[Boolean]): F[Unit] = ???
    }
}
