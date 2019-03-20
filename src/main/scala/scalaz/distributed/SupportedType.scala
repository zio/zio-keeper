package scalaz.distributed

private[distributed] trait SupportedType[A]

private[distributed] object SupportedType {
  implicit val bool   = new SupportedType[Boolean] {}
  implicit val int    = new SupportedType[Int]     {}
  implicit val long   = new SupportedType[Long]    {}
  implicit val string = new SupportedType[String]  {}

  implicit def set[A: SupportedType]: SupportedType[Set[A]] = new SupportedType[Set[A]] {}

  implicit def map[K: SupportedType, V: SupportedType]: SupportedType[Map[K, V]] =
    new SupportedType[Map[K, V]] {}
}
