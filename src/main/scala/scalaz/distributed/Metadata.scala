package scalaz.distributed

trait Metadata {
  def get[A: Type, B: Type](where: Path[A, B]): Distributed[B]
  def set[A: Type, B: Type](where: Path[A, B], b: B): Distributed[Unit]
}
