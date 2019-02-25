package scalaz

import java.net.InetAddress

import scalaz.zio.IO

trait Member

sealed trait Membership

object Membership {

  final case class Join(member: Member) extends Membership

  final case class Leave(member: Member) extends Membership

  final case class Unreachable(member: Member) extends Membership

}

sealed trait DistributedError

case class MetadataID(v: String)

trait DistributedModule {
  type F[A] = IO[DistributedError, A]

  type Type[A]

  type Path[A, B]

  implicit class PathSyntax[A: Type, B: Type](self: Path[A, B]) {
    def >>> [C: Type](that: Path[B, C]): Path[A, C] = compose[A, B, C](self, that)
  }

  def key[K: Type, V: Type](k: K): Path[Map[K, V], V]

  def compose[A: Type, B: Type, C: Type](l: Path[A, B], r: Path[B, C]): Path[A, C]

  implicit val stringType: Type[String]
  implicit val longType: Type[Long]
  implicit val intType: Type[Int]
  implicit val booleanType: Type[Boolean]
  implicit def mapType[K: Type, V: Type]: Type[Map[K, V]]

  def members(callback: Membership => F[Boolean]): F[Unit]

  def startup(member: Member, seed: Set[InetAddress]): F[Protocol]

  trait Protocol {
    def access[A: Type](id: MetadataID): F[Metadata[A]]
  }

  trait Metadata[A] {
    def set[B: Type](where: Path[A, B], b: B)(implicit A: Type[A]): F[Unit]

    final def modify[B: Type](where: Path[A, B], f: B => B)(implicit A: Type[A]): F[Unit] =
      for {
        c <- get[B](where)
        r <- set(where, f(c))
      } yield r

    def get[B: Type](where: Path[A, B])(implicit A: Type[A]): F[B]

    def increment(where: Path[A, Int])(implicit A: Type[A]): F[Unit]
  }

}
