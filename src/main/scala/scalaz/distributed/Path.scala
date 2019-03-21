package scalaz.distributed

sealed trait Path[A, B]

object Path {
  final case class Key[K: Type, V: Type](v: K) extends Path[Map[K, V], V]

  final case class Elements[A: Type]() extends Path[Set[A], A]

  final case class Composed[A: Type, B: Type, C: Type](
    x: Path[A, B],
    y: Path[B, C]
  ) extends Path[A, C]

  implicit class PathSyntax[A: Type, B: Type](self: Path[A, B]) {
    final def >>> [C: Type](that: Path[B, C]): Path[A, C] =
      Composed(self, that)
  }
}
