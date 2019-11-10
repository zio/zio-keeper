package zio.membership

import java.time.LocalDateTime

sealed trait NodeStateType
object NodeStateType {
  case object Alive extends NodeStateType
  case object Dead extends NodeStateType
  final case class Suspect(time: LocalDateTime) extends NodeStateType
}
