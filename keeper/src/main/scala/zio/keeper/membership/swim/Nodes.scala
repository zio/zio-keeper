package zio.keeper.membership.swim

import zio._
import zio.keeper.ClusterError.UnknownNode
import zio.keeper.membership.MembershipEvent.{ Join, NodeStateChanged }
import zio.keeper.membership.swim.Nodes.NodeState
import zio.keeper.membership.{ MembershipEvent, NodeAddress }
import zio.keeper.transport.{ Connection, Transport }
import zio.keeper.{ ByteCodec, Error }
import zio.logging.Logging
import zio.logging.slf4j._
import zio.nio.core.InetSocketAddress
import zio.stm.TMap
import zio.stream.{ Take, ZStream }

/**
 * Nodes maintains state of the cluster.
 *
 * @param local - local address
 * @param localId - local id
 * @param nodeChannels - connections
 * @param nodeStates - states
 * @param roundRobinOffset - offset for round-robin
 * @param transport - transport
 * @param messages - queue with messages
 */
class Nodes(
  val local: NodeAddress,
  localId: NodeId,
  nodeChannels: TMap[NodeId, Connection],
  nodeStates: TMap[NodeId, NodeState],
  roundRobinOffset: Ref[Int],
  transport: Transport.Service[Any],
  messages: Queue[Take[Error, Message]],
  eventsQueue: Queue[MembershipEvent]
) {

  /**
   * Accepts connection by adding new node in Init state.
   * @param connection transport connection
   * @return promise for init
   */
  final def accept(connection: Connection): ZIO[Any, Error, (NodeId, Fiber[Error, Unit])] =
    for {
      l            <- ByteCodec[NodeId].toChunk(localId)
      _            <- connection.send(l)
      firstMessage <- connection.read
      nodeId       <- ByteCodec[NodeId].fromChunk(firstMessage)
      messageCodec = ByteCodec[Message]
      _            <- (nodeChannels.put(nodeId, connection) *> nodeStates.put(nodeId, NodeState.Init)).commit
      fork <- ZStream
               .repeatEffect(connection.read.flatMap(messageCodec.fromChunk).map(_.copy(nodeId = nodeId)))
               .into(messages)
               .fork
    } yield (nodeId, fork)

  /**
   * Changes node state and issue membership event.
   * @param id - member id
   * @param newState - new state
   */
  def changeNodeState(id: NodeId, newState: NodeState): IO[Error, Unit] =
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
   * Initializes new connection with Init state.
   * @param addr - inet address
   * @return - NodeId of new member.
   */
  final def connect(addr: InetSocketAddress): ZIO[Logging[String], Error, NodeId] =
    logger.info("New connection: " + addr) *>
      Promise.make[Error, NodeId].flatMap { init =>
        transport
          .connect(addr)
          .use(
            conn =>
              accept(conn).foldM(
                err => init.fail(err),
                readFiber => init.succeed(readFiber._1) *> readFiber._2.join
              )
          )
          .catchAll(e => init.fail(e))
          .fork *>
          init.await
      }

  private def connection(id: NodeId): ZIO[Any, Error, Connection] =
    nodeChannels.get(id).commit.get.asError(UnknownNode(id))

  /**
   * close connection and remove Node from cluster.
   * @param id node id
   */
  def disconnect(id: NodeId): ZIO[Any, Error, Unit] =
    connection(id).flatMap(
      conn =>
        nodeChannels
          .delete(id)
          .zip(nodeStates.delete(id))
          .commit
          .unit *> conn.close
    )

  /**
   *  Stream of Membership Events
   */
  final val events: ZStream[Any, Nothing, MembershipEvent] =
    ZStream.fromQueue(eventsQueue)

  /**
   * Returns next Healthy node.
   */
  final val next: UIO[Option[NodeId]] /*(exclude: List[NodeId] = Nil)*/ =
    for {
      list      <- onlyHealthyNodes
      nextIndex <- roundRobinOffset.update(old => if (old < list.size - 1) old + 1 else 0)
    } yield list.drop(nextIndex).headOption.map(_._1)

  /**
   * Node state for given NodeId.
   */
  def nodeState(id: NodeId): IO[Error, NodeState] =
    nodeStates.get(id).commit.get.asError(UnknownNode(id))

  /**
   * Lists members that are in healthy state.
   */
  final def onlyHealthyNodes: UIO[List[(NodeId, NodeState)]] =
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
  final def send(msg: Message): IO[Error, Unit] =
    ByteCodec[Message]
      .toChunk(msg)
      .flatMap(
        chunk =>
          connection(msg.nodeId)
            .flatMap(_.send(chunk))
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
    localId: NodeId,
    messages: Queue[Take[Error, Message]]
  ): ZIO[Transport, Nothing, Nodes] =
    for {
      nodeChannels     <- TMap.empty[NodeId, Connection].commit
      nodeStates       <- TMap.empty[NodeId, NodeState].commit
      events           <- Queue.sliding[MembershipEvent](100)
      roundRobinOffset <- Ref.make(0)
      env              <- ZIO.environment[Transport]
    } yield new Nodes(
      local,
      localId,
      nodeChannels,
      nodeStates,
      roundRobinOffset,
      env.transport,
      messages,
      events
    )

}
