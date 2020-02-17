package zio.keeper.membership.swim

import zio.keeper.ClusterError.UnknownNode
import zio.keeper.membership.NodeAddress
import zio.keeper.transport.{ Connection, Transport }
import zio.keeper.{ Error, Message }
import zio.logging.Logging
import zio.logging.slf4j._
import zio.nio.core.InetSocketAddress
import zio.stm.{ STM, TMap }
import zio.stream.{ Take, ZStream }
import zio._

class Nodes(
  val local: NodeAddress,
  nodeChannels: TMap[NodeAddress, ClusterConnection],
  state: Ref[GossipState],
  roundRobinOffset: Ref[Int],
  transport: Transport.Service[Any],
  messages: zio.Queue[Take[Error, (ClusterConnection, Message)]]
) {

  final def accept(connection: Connection): ZIO[Any, Error, ClusterConnection] =
    NodeAddress(connection.address)
      .map(addr => new ClusterConnection(connection, addr))
      .tap(conn => nodeChannels.put(conn.address, conn).commit)

  final def connect(addr: InetSocketAddress): ZIO[Logging[String], Error, Unit] =
    logger.info("New connection: " + addr) *>
      NodeAddress(addr).zip(Promise.make[Error, Unit]).flatMap {
        case (nodeAddress, init) =>
          transport
            .connect(addr)
            .map(new ClusterConnection(_, nodeAddress))
            .use(
              conn =>
                nodeChannels.put(nodeAddress, conn).commit *>
                  logger.info("Node: " + nodeAddress + " added to cluster.") *>
                  init.succeed(()) *>
                  ZStream.repeatEffect(conn.read.map(msg => (conn, msg))).into(messages)

            )
            .fork *>
            init.await
      }

  final def connection(addr: NodeAddress): ZIO[Any, Error, ClusterConnection] =
    nodeChannels.get(addr).commit.get.asError(UnknownNode(addr))

  def currentState: UIO[GossipState] =
    state.get

  def disconnect(sender: NodeAddress): _root_.zio.ZIO[Any, Error, Unit] =
    nodeChannels.delete(sender).commit

  final def established(addr: NodeAddress, alias: NodeAddress): ZIO[Any, Error, Unit] =
    nodeChannels
      .get(addr)
      .flatMap {
        case None             => STM.fail(UnknownNode(addr))
        case Some(connection) => nodeChannels.put(alias, connection)
      }
      .commit *> state.update(_.addMember(addr)).unit

  final def next /*(exclude: List[NodeId] = Nil)*/ =
    for {
      nodes     <- nodeChannels.keys.commit
      nextIndex <- roundRobinOffset.update(old => if (old < nodes.size - 1) old + 1 else 0)
    } yield nodes.drop(nextIndex).headOption

  def updateState(newState: GossipState): ZIO[Logging[String], Nothing, Unit] =
    for {
      current <- state.get
      diff    = newState.diff(current)
      _       <- state.set(newState.merge(current))
      _       <- ZIO.foreach(diff.local)(n => n.socketAddress.flatMap(connect).ignore)
    } yield ()

}

object Nodes {

  sealed trait NodeState

  object NodeState {
    case object Unknown     extends NodeState
    case object Accepted    extends NodeState
    case object Established extends NodeState
  }

  def make(local: NodeAddress) =
    for {
      nodeChannels     <- TMap.empty[NodeAddress, ClusterConnection].commit
      gossipState      <- Ref.make(GossipState.Empty)
      roundRobinOffset <- Ref.make(0)
      messages         <- Queue.unbounded[Take[Error, (ClusterConnection, Message)]]
      env              <- ZIO.environment[Transport]
    } yield new Nodes(
      local,
      nodeChannels,
      gossipState,
      roundRobinOffset,
      env.transport,
      messages
    )

}
