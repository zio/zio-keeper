package zio.keeper.membership.swim

import zio.keeper.transport.Transport
import zio.keeper.{Error, Message}
import zio.stm.TMap
import zio.stream.{Take, ZStream}
import zio.{Ref, UIO, ZIO}

abstract class Nodes[A](
  val local: A,
  nodeChannels: TMap[A, ClusterConnection],
  state: Ref[GossipState[A]],
  roundRobinOffset: Ref[Int],
  transport: Transport[A],
  messages: zio.Queue[Take[Error, (ClusterConnection, Message)]]
) {

  final def connect(addr: A): ZIO[Any, Error, Unit] =
    transport.transport
      .connect(addr)
      .map(new ClusterConnection(_))
      .use(
        conn =>
          nodeChannels.put(addr, conn).commit *>
            ZStream.repeatEffect(conn.read.map(msg => (conn, msg))).into(messages)
      )
      .fork
      .unit

  final def next /*(exclude: List[NodeId] = Nil)*/ =
    for {
      nodes     <- nodeChannels.keys.commit
      nextIndex <- roundRobinOffset.update(old => if (old < nodes.size - 1) old + 1 else 0)
    } yield nodes.drop(nextIndex).headOption

  final def established(addr: A): ZIO[Any, Error, Unit] =
    state.update(_.addMember(addr)).unit

  def updateState(newState: GossipState[A]): ZIO[Any, Nothing, Unit] =
    for {
      current <- state.get
      diff    = newState.diff(current)
      _       <- ZIO.foreach(diff.local)(n => connect(n).ignore)
    } yield ()

  def currentState: UIO[GossipState[A]] = state.get
}
