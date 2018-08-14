package scalaz

trait Member

sealed trait Membership
object Membership {
  final case class Join(member: Member) extends Membership
  final case class Leave(member: Member) extends Membership
  final case class Unreachable(member: Member) extends Membership
}

sealed trait ValueChanged[A]
object ValueChanged {
  final case class Modify[A](member: Member, old: A, newV: A) extends ValueChanged[A]
  final case class Add[A](member: Member, newV: A) extends ValueChanged[A]
  final case class Remove[A](member: Member, value: A) extends ValueChanged[A]
}

trait DistributedModule[F[_]] {
  type Type[A]
  type Prism[A, B]
  type Traversal[A, B]

  sealed trait PathElem[A, B]
  object PathElem {
    case class Key[K, V](v: K) extends PathElem[Map[K, V], V]
    case class MetadataId[A](v: String) extends PathElem[A, A]
    case class Elements[A]() extends PathElem[Set[A], A]
    case class Composed[A, B, C](l: PathElem[A, B], r: PathElem[B, C]) extends PathElem[A, C]
  }

  implicit val stringType   : Type[String]
  implicit val longType     : Type[Long]
  implicit val booleanType  : Type[Boolean]
  implicit def setType[V: Type] : Type[Set[V]]
  implicit def mapType[K: Type, V: Type]: Type[Map[K, V]]

  implicit class LensSyntax[A: Type, B: Type](self: PathElem[A, B]) {
    def >>> [C: Type](that: PathElem[B, C]): PathElem[A, C] = PathElem.Composed(self, that)
  }

  def members(callback: Membership => F[Boolean]): F[Unit]

  def access[A: Type, B: Type](id: PathElem[A, B]): F[Metadata[B]]

  def onChange[A: Type, B: Type](p: PathElem[A, B], callback: ValueChanged[B] => F[Unit]): F[Unit]

  def key[K: Type, V: Type](k: K): PathElem[Map[K, V], V] = PathElem.Key[K, V](k)

  def elements[V: Type]: PathElem[Set[V], V] = PathElem.Elements[V]()

  def metadataId[A: Type](s: String): PathElem[A, A] = PathElem.MetadataId[A](s)

  trait Metadata[A] {
    def mod(a: A => A): F[Unit]
    def get: F[A]
  }
}
