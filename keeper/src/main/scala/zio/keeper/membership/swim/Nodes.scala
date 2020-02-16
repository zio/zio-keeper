package zio.keeper.membership.swim

import zio.keeper.ClusterError.UnknownNode
import zio.keeper.transport.Transport
import zio.keeper.{ByteCodec, Error, Message}
import zio.logging.Logging
import zio.stm.TMap
import zio.stream.{Take, ZStream}
import zio.{Queue, Ref, UIO, ZIO}
import zio.logging.slf4j._

class Nodes[A: ByteCodec](
  val local: A,
  nodeChannels: TMap[A, ClusterConnection[A]],
  state: Ref[GossipState[A]],
  roundRobinOffset: Ref[Int],
  transport: Transport.Service[Any, A],
  messages: zio.Queue[Take[Error, (ClusterConnection[A], Message)]]
) {

  final def accept(connection: ClusterConnection[A]): ZIO[Any, Error, Unit] =
    nodeChannels.put(connection.address, connection).commit

  final def connect(addr: A): ZIO[Logging[String], Error, Unit] =
    logger.info("New connection: " + addr) *>
    transport
      .connect(addr)
      .map(new ClusterConnection(_))
      .use(
        conn =>
          nodeChannels.put(addr, conn).commit *>
            ZStream.repeatEffect(conn.read.map(msg => (conn, msg))).into(messages)
      )
      .fork
      .unit

  final def connection(addr: A): ZIO[Any, Error, ClusterConnection[A]] =
    nodeChannels.get(addr).commit.get.asError(UnknownNode(addr))

  def currentState: UIO[GossipState[A]] =
    state.get

  def disconnect(sender: A): _root_.zio.ZIO[Any, Error, Unit] =
    nodeChannels.delete(sender).commit

  final def established(addr: A): ZIO[Any, Error, Unit] =
    state.update(_.addMember(addr)).unit

  final def next /*(exclude: List[NodeId] = Nil)*/ =
    for {
      nodes     <- nodeChannels.keys.commit
      nextIndex <- roundRobinOffset.update(old => if (old < nodes.size - 1) old + 1 else 0)
    } yield nodes.drop(nextIndex).headOption

  def updateState(newState: GossipState[A]): ZIO[Logging[String], Nothing, Unit] =
    for {
      current <- state.get
      diff    = newState.diff(current)
      _       <- state.set(newState.merge(current))
      _       <- ZIO.foreach(diff.local)(n => connect(n).ignore)
    } yield ()

}

object Nodes {

  def make[A: ByteCodec](local: A) =
    for {
      nodeChannels     <- TMap.empty[A, ClusterConnection[A]].commit
      gossipState      <- Ref.make(GossipState.Empty[A])
      roundRobinOffset <- Ref.make(0)
      messages         <- Queue.unbounded[Take[Error, (ClusterConnection[A], Message)]]
      env              <- ZIO.environment[Transport[A]]
    } yield new Nodes[A](
      local,
      nodeChannels,
      gossipState,
      roundRobinOffset,
      env.transport,
      messages
    )

}
