package scalaz

import java.net.InetAddress

private[scalaz] class DistributedModuleImpl extends DistributedModule {

  type Path[A, B] = PathElem[A, B]

  sealed class TypeImpl[A]

  type Type[A] = TypeImpl[A]

  sealed trait PathElem[A, B]

  object PathElem {

    case class Key[K: Type, V: Type](v: K) extends PathElem[Map[K, V], V]

    case class Elements[A: Type]() extends PathElem[Set[A], A]

    case class Composed[A: Type, B: Type, C: Type](l: PathElem[A, B], r: PathElem[B, C])
        extends PathElem[A, C]

  }

  override def key[K: Type, V: Type](k: K): Path[Map[K, V], V] =
    PathElem.Key[K, V](k)

  override def compose[A: Type, B: Type, C: Type](l: Path[A, B], r: Path[B, C]): Path[A, C] =
    PathElem.Composed(l, r)

  implicit val stringType: Type[String] = new TypeImpl[String]

  implicit val longType: Type[Long] = new TypeImpl[Long]

  implicit val intType: Type[Int] = new TypeImpl[Int]

  implicit val booleanType: Type[Boolean] = new TypeImpl[Boolean]

  implicit def mapType[K: Type, V: Type]: Type[Map[K, V]] = new TypeImpl[Map[K, V]]

  override def members(callback: Membership => F[Boolean]): F[Unit] = ???

  override def startup(member: Member, seed: Set[InetAddress]): F[Protocol] = ???
}
