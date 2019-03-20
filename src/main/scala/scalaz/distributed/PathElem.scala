package scalaz.distributed

sealed private[distributed] trait PathElem[A, B]

private[distributed] object PathElem {
  final case class Key[K: SupportedType, V: SupportedType](v: K) extends PathElem[Map[K, V], V]

  final case class Elements[A: SupportedType]() extends PathElem[Set[A], A]

  final case class Composed[A: SupportedType, B: SupportedType, C: SupportedType](
    x: PathElem[A, B],
    y: PathElem[B, C]
  ) extends PathElem[A, C]

  implicit class PathSyntax[A: SupportedType, B: SupportedType](self: PathElem[A, B]) {
    final def >>> [C: SupportedType](that: PathElem[B, C]): PathElem[A, C] = Composed(self, that)
  }
}
