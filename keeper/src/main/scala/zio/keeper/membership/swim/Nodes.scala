package zio.keeper.membership.swim

import zio._
import zio.keeper.ClusterError.UnknownNode
import zio.keeper.Error
import zio.keeper.membership.MembershipEvent.{ Join, NodeStateChanged }
import zio.keeper.membership.swim.Nodes.NodeState
import zio.keeper.membership.{ ByteCodec, MembershipEvent, NodeAddress }
import zio.keeper.transport.Channel.Connection
import zio.keeper.transport.Transport
import zio.stm.TMap
import zio.stream.{ Take, ZStream }

/**
 * Nodes maintains state of the cluster.
 *
 * @param local - local address
 * @param nodeStates - states
 * @param roundRobinOffset - offset for round-robin
 * @param transport - transport
 * @param messages - queue with messages
 */
class Nodes(
  val local: NodeAddress,
  nodeStates: TMap[NodeAddress, NodeState],
  roundRobinOffset: Ref[Int],
  transport: Transport.Service,
  messages: Queue[Take[Error, Message.Direct[Chunk[Byte]]]],
  eventsQueue: Queue[MembershipEvent]
) {

  /**
   * Reads message and put into queue.
   * @param connection transport connection
   */
  final def read(connection: Connection): ZIO[Any, Error, Unit] =
    Take
      .fromEffect(
        connection.read
          .flatMap(ByteCodec[Message.Direct[Chunk[Byte]]].fromChunk)
          .tap(msg => addNode(msg.node))
      )
      .flatMap(messages.offer)
      .tap(_ => connection.close)
      .unit

  def addNode(node: NodeAddress) =
    nodeStates
      .put(node, NodeState.Init)
      .whenM(nodeStates.contains(node).map(!_))
      .commit
      .unit

  /**
   * Changes node state and issue membership event.
   * @param id - member id
   * @param newState - new state
   */
  def changeNodeState(id: NodeAddress, newState: NodeState): IO[Error, Unit] =
    nodeState(id)
      .flatMap { prev =>
        nodeStates
          .put(id, newState)
          .commit
          .as(
            if (newState == NodeState.Healthy && prev == NodeState.Init) {
              Join(id)
            } else {
              NodeStateChanged(id, prev, newState)
            }
          )
      }
      .flatMap(eventsQueue.offer)
      .unit

  /**
   * close connection and remove Node from cluster.
   * @param id node id
   */
  def disconnect(id: NodeAddress): ZIO[Any, Error, Unit] =
    nodeStates.delete(id).commit

  /**
   *  Stream of Membership Events
   */
  final val events: ZStream[Any, Nothing, MembershipEvent] =
    ZStream.fromQueue(eventsQueue)

  /**
   * Returns next Healthy node.
   */
  final val next: UIO[Option[NodeAddress]] /*(exclude: List[NodeId] = Nil)*/ =
    for {
      list      <- onlyHealthyNodes
      nextIndex <- roundRobinOffset.updateAndGet(old => if (old < list.size - 1) old + 1 else 0)
    } yield list.drop(nextIndex).headOption.map(_._1)

  /**
   * Node state for given NodeId.
   */
  def nodeState(id: NodeAddress): IO[Error, NodeState] =
    nodeStates.get(id).commit.get.orElseFail(UnknownNode(id))

  /**
   * Lists members that are in healthy state.
   */
  final def onlyHealthyNodes: UIO[List[(NodeAddress, NodeState)]] =
    nodeStates.toList.map(_.filter(_._2 == NodeState.Healthy)).commit

  /**
   * Returns string with cluster state.
   */
  final val prettyPrint: UIO[String] =
    nodeStates.toList.commit.map(
      nodes =>
        "[ size: " + nodes.size +
          " nodes: [" +
          nodes
            .map {
              case (address, nodeState) =>
                "address: " + address + " state: " + nodeState
            }
            .mkString("|") +
          "]]"
    )

  /**
   * Sends message to target.
   */
  final def send(msg: Message.Direct[Chunk[Byte]]): IO[Error, Unit] =
    ByteCodec[Message.Direct[Chunk[Byte]]]
      .toChunk(msg.copy(node = local))
      .flatMap(
        chunk => msg.node.socketAddress.toManaged_.flatMap(transport.connect).use(_.send(chunk))
      )

}

object Nodes {

  sealed trait NodeState

  object NodeState {
    case object Init        extends NodeState
    case object Healthy     extends NodeState
    case object Unreachable extends NodeState
    case object Suspicion   extends NodeState
  }

  def make(
    local: NodeAddress,
    messages: Queue[Take[Error, Message.Direct[Chunk[Byte]]]],
    udpTransport: Transport.Service
  ): ZIO[Any, Nothing, Nodes] =
    for {
      nodeStates       <- TMap.empty[NodeAddress, NodeState].commit
      events           <- Queue.sliding[MembershipEvent](100)
      roundRobinOffset <- Ref.make(0)
    } yield new Nodes(
      local,
      nodeStates,
      roundRobinOffset,
      udpTransport,
      messages,
      events
    )

}
