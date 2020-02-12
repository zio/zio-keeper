package zio.keeper.membership.swim

import zio.keeper.membership.NodeId
import zio.keeper.transport.Transport
import zio.keeper.{ Error, Message }
import zio.stm.TMap
import zio.stream.{ Take, ZStream }
import zio.{ Ref, ZIO }

abstract class Nodes[A](
  nodeChannels: TMap[NodeId, ClusterConnection],
  roundRobinOffset: Ref[Int],
  transport: Transport[A],
  messages: zio.Queue[Take[Error, (ClusterConnection, Message)]]
) {

  final def connect(nodeId: NodeId, addr: A): ZIO[Any, Error, Unit] =
    transport.transport
      .connect(addr)
      .map(new ClusterConnection(_))
      .use(
        conn =>
          nodeChannels.put(nodeId, conn).commit *>
            ZStream.repeatEffect(conn.read.map(msg => (conn, msg))).into(messages)
      )
      .fork
      .unit

  final def next /*(exclude: List[NodeId] = Nil)*/ =
    for {
      nodes     <- nodeChannels.keys.commit
      nextIndex <- roundRobinOffset.update(old => if (old < nodes.size - 1) old + 1 else 0)
    } yield nodes.drop(nextIndex).headOption

}
