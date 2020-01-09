package zio.membership

sealed trait MessageType

object MessageType {

  case object Cluster extends MessageType

  case object User extends MessageType

}
