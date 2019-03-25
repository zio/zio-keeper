package scalaz.ziokeeper

import java.net.InetAddress

import scala.collection.immutable

trait Client {
  type Path[_, _]
  type Type[_]

  object Type {
    def apply[A](implicit instance: Type[A]): Type[A] = instance
  }

  implicit val bool: Type[scala.Boolean]
  implicit val int: Type[scala.Int]
  implicit val long: Type[scala.Long]
  implicit val double: Type[scala.Double]
  implicit val string: Type[scala.Predef.String]
  implicit def set[A: Type]: Type[immutable.Set[A]]
  implicit def map[K: Type, V: Type]: Type[immutable.Map[K, V]]

  trait Protocol {
    def access[A: Type](id: MetadataID): Distributed[Metadata]
  }

  trait Metadata {
    def get[A: Type, B: Type](where: Path[A, B]): Distributed[B]
    def set[A: Type, B: Type](where: Path[A, B], b: B): Distributed[Unit]
  }

  def key[K: Type, V: Type](k: K): Path[Map[K, V], V]

  def compose[A: Type, B: Type, C: Type](x: Path[A, B], y: Path[B, C]): Path[A, C]

  def connect(member: Member, seed: Set[InetAddress]): Distributed[Protocol]

  def members(cb: Membership => Distributed[Boolean]): Distributed[Unit]
}

object Client {
  private[ziokeeper] def default: Client = new Client {
    override type Type[A]    = InternalType[A]
    override type Path[A, B] = InternalPath[A, B]

    sealed trait InternalPath[A, B]

    object InternalPath {
      case class Key[K: Type, V: Type](v: K) extends InternalPath[Map[K, V], V]

      case class Elements[A: Type]() extends InternalPath[Set[A], A]

      case class Composed[A: Type, B: Type, C: Type](x: Path[A, B], y: Path[B, C])
          extends InternalPath[A, C]
    }

    implicit class PathSyntax[A: Type, B: Type](self: Path[A, B]) {
      final def >>> [C: Type](that: Path[B, C]): Path[A, C] = InternalPath.Composed(self, that)
    }

    trait InternalType[A] {
      def reified: Reified
    }

    object InternalType {
      private[Client] def apply[A](r: Reified): InternalType[A] =
        new InternalType[A] {
          override def reified: Reified = r
        }
    }

    implicit override val bool: Type[scala.Boolean]         = InternalType(Reified.Bool)
    implicit override val int: Type[scala.Int]              = InternalType(Reified.Int)
    implicit override val long: Type[scala.Long]            = InternalType(Reified.Long)
    implicit override val double: Type[scala.Double]        = InternalType(Reified.Double)
    implicit override val string: Type[scala.Predef.String] = InternalType(Reified.String)

    implicit override def set[A: Type]: Type[immutable.Set[A]] =
      InternalType(Reified.Set(Type[A].reified))

    implicit override def map[K: Type, V: Type]: Type[immutable.Map[K, V]] =
      InternalType(Reified.Map(Type[K].reified, Type[V].reified))

    def key[K: Type, V: Type](k: K): Path[Map[K, V], V] = InternalPath.Key[K, V](k)

    def compose[A: Type, B: Type, C: Type](x: Path[A, B], y: Path[B, C]): Path[A, C] = x >>> y

    def connect(member: Member, seed: Set[InetAddress]): Distributed[Protocol] = ???

    def members(cb: Membership => Distributed[Boolean]): Distributed[Unit] = ???
  }
}
