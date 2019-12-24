package zio.membership

import java.{util => ju}

final case class NodeId(value: ju.UUID) extends AnyVal
