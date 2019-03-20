package scalaz.distributed

trait Metadata[Type[_], Path[_, _]] {
  def get[A: Type, B: Type](where: Path[A, B]): F[B]
  def set[A: Type, B: Type](where: Path[A, B], b: B): F[Unit]

  final def modify[A: Type, B: Type](where: Path[A, B], f: B => B): F[Unit] =
    for {
      c <- get(where)
      r <- set(where, f(c))
    } yield r

  def increment[A: Type](where: Path[A, Int]): F[Unit]
}
