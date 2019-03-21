package scalaz.distributed

trait Type[A] {
  def reified: Reified
}

object Type {
  def apply[A](implicit instance: Type[A]): Type[A] = instance
}
