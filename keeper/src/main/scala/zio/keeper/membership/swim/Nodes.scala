package zio.keeper.membership.swim

import zio._
import zio.keeper.ClusterError.UnknownNode
import zio.keeper.membership.swim.Nodes.NodeState
import zio.keeper.membership.{ MembershipEvent, NodeAddress }
import zio.keeper.transport.{ Connection, Transport }
import zio.keeper.{ ByteCodec, Error }
import zio.logging.Logging
import zio.logging.slf4j._
import zio.nio.core.InetSocketAddress
import zio.stm.{ STM, TMap }
import zio.stream.{ Take, ZStream }

class Nodes(
  val local: NodeAddress,
  val localId: NodeId,
  nodeChannels: TMap[NodeId, Connection],
  nodeStates: TMap[NodeId, NodeState],
  roundRobinOffset: Ref[Int],
  transport: Transport.Service[Any],
  messages: zio.Queue[Take[Error, (NodeId, Chunk[Byte])]]
) {

  def nodeState(target: NodeId) =
    nodeStates.get(target).commit.get.asError(UnknownNode(target))

  def changeNodeState(target: NodeId, newState: NodeState) =
    nodeStates.put(target, newState).commit

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
      _            <- (nodeChannels.put(nodeId, connection) *> nodeStates.put(nodeId, NodeState.Init)).commit
      fork <- ZStream
               .repeatEffect(connection.read)
               .map((nodeId, _))
               .into(messages)
               .fork
    } yield (nodeId, fork)

  final def send(msg: (NodeId, Chunk[Byte])) =
    connection(msg._1)
      .flatMap(_.send(msg._2))

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
          .fork *>
          init.await
      }

  private def connection(addr: NodeId): ZIO[Any, Error, Connection] =
    nodeChannels.get(addr).commit.get.asError(UnknownNode(addr))

  def disconnect(address: NodeId): _root_.zio.ZIO[Any, Error, Unit] =
    nodeChannels
      .delete(address)
      .zip(nodeStates.delete(address))
      .commit
      .unit

  final def established(nodeId: NodeId): ZIO[Any, Error, Unit] =
    nodeChannels
      .get(nodeId)
      .flatMap {
        case None => STM.fail(UnknownNode(nodeId))
        case Some(_) =>
          nodeStates.put(nodeId, NodeState.Healthy)
      }
      .commit
      .unit

  final def next /*(exclude: List[NodeId] = Nil)*/ =
    for {
      onlyHealthy <- onlyHealthyNodes
      nextIndex   <- roundRobinOffset.update(old => if (old < onlyHealthy.size - 1) old + 1 else 0)
    } yield onlyHealthy.drop(nextIndex).headOption.map(_._1)

  final val onlyHealthyNodes =
    nodeStates.toList.map(_.filter(_._2 == NodeState.Healthy)).commit

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

  final def events: ZStream[Any, Nothing, MembershipEvent] = ???
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

  def make(local: NodeAddress, localId: NodeId, messages: Queue[Take[Error, (NodeId, Chunk[Byte])]]) =
    for {
      nodeChannels     <- TMap.empty[NodeId, Connection].commit
      nodeStates       <- TMap.empty[NodeId, NodeState].commit
      roundRobinOffset <- Ref.make(0)
      env              <- ZIO.environment[Transport]
    } yield new Nodes(
      local,
      localId,
      nodeChannels,
      nodeStates,
      roundRobinOffset,
      env.transport,
      messages
    )

}
