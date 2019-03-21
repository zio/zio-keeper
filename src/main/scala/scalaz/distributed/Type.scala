package scalaz.distributed

trait Type[A] {
  def reified: Reified
}

object Type {
  def apply[A](implicit instance: Type[A]): Type[A] = instance

  implicit val bool: Type[scala.Boolean]         = instanceOf(Reified.Bool)
  implicit val int: Type[scala.Int]              = instanceOf(Reified.Int)
  implicit val long: Type[scala.Long]            = instanceOf(Reified.Long)
  implicit val double: Type[scala.Double]        = instanceOf(Reified.Double)
  implicit val string: Type[scala.Predef.String] = instanceOf(Reified.String)

  private def instanceOf[A](r: Reified): Type[A] =
    new Type[A] {
      override def reified: Reified = r
    }
}
