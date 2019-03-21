package scalaz

package object distributed {
  type Distributed[A] = IO[Error, A]

  implicit class PathSyntax[A: SupportedType, B: SupportedType](self: PathElem[A, B]) {
    final def >>> [C: SupportedType](that: PathElem[B, C]): PathElem[A, C] =
      PathElem.Composed(self, that)
  }
}
