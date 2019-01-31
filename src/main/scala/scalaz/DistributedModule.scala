package scalaz

import java.net.InetAddress

import scalaz.zio.IO

trait Member

sealed trait Membership

object Membership {
  final case class Join(member: Member)        extends Membership
  final case class Leave(member: Member)       extends Membership
  final case class Unreachable(member: Member) extends Membership
}

sealed trait DistributedError

case class MetadataID(v: String)

trait DistributedModule {

  type F[A] = IO[DistributedError, A]

  type Type[A]

  type Lens[A, B]
  type Prism[A, B]
  type Traversal[A, B]

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
    def set[B: Type](where: Lens[A, B], b: B)(implicit A: Type[A]): F[Unit]
    def get[B: Type](where: Lens[A, B])(implicit A: Type[A]): F[B]
    def increment(where: Lens[A, Int])(implicit A: Type[A]): F[Unit]
  }

  def key[K, V](k: K): Lens[Map[K, V], V]
}


