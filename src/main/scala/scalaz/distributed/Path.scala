package scalaz.distributed

sealed trait Path[A, B]

object Path {
  final case class Key[K: SupportedType, V: SupportedType](v: K) extends Path[Map[K, V], V]

  final case class Elements[A: SupportedType]() extends Path[Set[A], A]

  final case class Composed[A: SupportedType, B: SupportedType, C: SupportedType](
    x: Path[A, B],
    y: Path[B, C]
  ) extends Path[A, C]

  implicit class PathSyntax[A: SupportedType, B: SupportedType](self: Path[A, B]) {
    final def >>> [C: SupportedType](that: Path[B, C]): Path[A, C] =
      Composed(self, that)
  }
}
