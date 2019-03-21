package scalaz.distributed

import scala.collection.immutable

trait Type[A] {
  def reified: Reified
}

object Type {
  def apply[A](implicit instance: Type[A]): Type[A] = instance

  implicit val bool: Type[scala.Boolean]         = typeOf(Reified.Bool)
  implicit val int: Type[scala.Int]              = typeOf(Reified.Int)
  implicit val long: Type[scala.Long]            = typeOf(Reified.Long)
  implicit val double: Type[scala.Double]        = typeOf(Reified.Double)
  implicit val string: Type[scala.Predef.String] = typeOf(Reified.String)

  implicit def set[A: Type]: Type[immutable.Set[A]] =
    typeOf(Reified.Set(Type[A].reified))

  implicit def map[K: Type, V: Type]: Type[immutable.Map[K, V]] =
    typeOf(Reified.Map(Type[K].reified, Type[V].reified))

  private def typeOf[A](r: Reified): Type[A] =
    new Type[A] {
      override def reified: Reified = r
    }
}
