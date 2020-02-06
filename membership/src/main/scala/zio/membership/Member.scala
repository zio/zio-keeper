package zio.membership

import upickle.default._


final case class Member[A](nodeId: NodeId, addr: A)

object Member {
  implicit def ordering[A]: Ordering[Member[A]] = Ordering.by(_.nodeId)
  implicit def memberRW[A: ReadWriter] = macroRW[Member[A]]
}

