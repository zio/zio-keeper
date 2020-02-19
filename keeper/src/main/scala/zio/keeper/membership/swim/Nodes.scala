package zio.keeper.membership.swim

import zio._
import zio.keeper.ClusterError.UnknownNode
import zio.keeper.Error
import zio.keeper.membership.NodeAddress
import zio.keeper.membership.swim.Nodes.NodeState
import zio.keeper.transport.{ Connection, Transport }
import zio.logging.Logging
import zio.logging.slf4j._
import zio.nio.core.InetSocketAddress
import zio.stm.{ STM, TMap }
import zio.stream.{ Take, ZStream }

class Nodes(
  val local: NodeAddress,
  nodeChannels: TMap[NodeAddress, Connection],
  nodeStates: TMap[NodeAddress, NodeState],
  state: Ref[GossipState],
  roundRobinOffset: Ref[Int],
  transport: Transport.Service[Any],
  messages: zio.Queue[Take[Error, (NodeAddress, Chunk[Byte])]]
) {

  def nodeState(target: NodeAddress) =
    nodeStates.get(target).commit.get.asError(UnknownNode(target))

  def changeNodeState(target: NodeAddress, newState: NodeState) =
    nodeStates.put(target, newState).commit

  final def accept(nodeAddress: NodeAddress, connection: Connection): ZIO[Any, Error, Connection] =
    (nodeChannels.put(nodeAddress, connection) *>
      nodeStates.put(nodeAddress, NodeState.Init)).commit.as(connection)

  final def send(msg: (NodeAddress, Chunk[Byte])) =
    connection(msg._1)
      .flatMap(_.send(msg._2))

  final def connect(addr: InetSocketAddress): ZIO[Logging[String], Error, Unit] =
    logger.info("New connection: " + addr) *>
      NodeAddress(addr).zip(Promise.make[Error, Unit]).flatMap {
        case (nodeAddress, init) =>
          transport
            .connect(addr)
            .use(
              conn =>
                nodeChannels.put(nodeAddress, conn).commit *>
                  logger.info("Node: " + nodeAddress + " added to cluster.") *>
                  init.succeed(()) *>
                  ZStream.repeatEffect(conn.read.map(msg => (nodeAddress, msg))).into(messages)
            )
            .fork *>
            init.await
      }

  final def connection(addr: NodeAddress): ZIO[Any, Error, Connection] =
    nodeChannels.get(addr).commit.get.asError(UnknownNode(addr))

  def currentState: UIO[GossipState] =
    state.get

  def disconnect(address: NodeAddress): _root_.zio.ZIO[Any, Error, Unit] =
    nodeChannels
      .delete(address)
      .zip(nodeStates.delete(address))
      .commit
      .unit

  final def established(addr: NodeAddress, alias: NodeAddress): ZIO[Any, Error, Unit] =
    nodeChannels
      .get(addr)
      .flatMap {
        case None => STM.fail(UnknownNode(addr))
        case Some(connection) =>
          nodeChannels.put(alias, connection) *>
            nodeStates.put(alias, NodeState.Healthy) *>
            nodeStates.put(addr, NodeState.Healthy)
      }
      .commit *> state.update(_.addMember(alias)).unit

  final def next /*(exclude: List[NodeId] = Nil)*/ =
    for {
      nodes     <- nodeChannels.keys.commit
      nextIndex <- roundRobinOffset.update(old => if (old < nodes.size - 1) old + 1 else 0)
    } yield nodes.drop(nextIndex).headOption
}

object Nodes {

  sealed trait NodeState

  object NodeState {
    case object Unknown     extends NodeState
    case object Init        extends NodeState
    case object Healthy     extends NodeState
    case object Unreachable extends NodeState
    case object Suspicion   extends NodeState
  }

  def make(local: NodeAddress, messages: Queue[Take[Error, (NodeAddress, Chunk[Byte])]]) =
    for {
      nodeChannels     <- TMap.empty[NodeAddress, Connection].commit
      nodeStates       <- TMap.empty[NodeAddress, NodeState].commit
      gossipState      <- Ref.make(GossipState.Empty)
      roundRobinOffset <- Ref.make(0)
      env              <- ZIO.environment[Transport]
    } yield new Nodes(
      local,
      nodeChannels,
      nodeStates,
      gossipState,
      roundRobinOffset,
      env.transport,
      messages
    )

}
