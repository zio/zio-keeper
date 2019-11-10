package zio.membership

final case class Sender(
  nodeId: NodeId,
  convId: Option[Long] // this will be used to associate messages with responses
)
