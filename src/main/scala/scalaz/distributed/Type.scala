package scalaz.distributed

trait Type[A]

object Type {
  implicit val bool   = new Type[Boolean] {}
  implicit val int    = new Type[Int]     {}
  implicit val long   = new Type[Long]    {}
  implicit val string = new Type[String]  {}

  implicit def set[A: Type]: Type[Set[A]] = new Type[Set[A]] {}

  implicit def map[K: Type, V: Type]: Type[Map[K, V]] = new Type[Map[K, V]] {}
}
