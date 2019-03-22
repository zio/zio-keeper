package scalaz.distributed

sealed trait Reified

object Reified {
  case object Bool   extends Reified
  case object Int    extends Reified
  case object Long   extends Reified
  case object Double extends Reified
  case object String extends Reified

  final case class Set(a: Reified)             extends Reified
  final case class Map(k: Reified, v: Reified) extends Reified
}
