package zio.membership


final case class Member[A](nodeId: NodeId, addr: A)


