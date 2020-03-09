package zio.keeper.membership.swim


sealed trait Message[+A]

object Message {
  case class Direct[A](nodeId: NodeId, message: A) extends Message[A] {
    def reply[B <: A](message: B) =
      this.copy(message = message)
  }
  case class Gossip[A](message: A)                 extends Message[A]
  case object Empty                           extends Message[Nothing]
}
