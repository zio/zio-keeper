package zio.membership

import java.{util => ju}

import upickle.default.macroRW
final case class NodeId(value: ju.UUID) extends AnyVal

object NodeId {
  implicit val ordering: Ordering[NodeId] = Ordering.by(_.value)
  implicit val nodeIdRW = macroRW[NodeId]
}

