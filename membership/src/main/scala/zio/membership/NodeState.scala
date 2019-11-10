package zio.membership

final case class NodeState(
  node: NodeId,
  incarnation: Int,
  state: NodeStateType
)
