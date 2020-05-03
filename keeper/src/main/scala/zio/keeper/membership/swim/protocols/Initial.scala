package zio.keeper.membership.swim.protocols

import upickle.default._
import zio.ZIO
import zio.keeper.{ ByteCodec, NodeAddress, TaggedCodec }
import zio.keeper.discovery.Discovery
import zio.keeper.membership.swim.Nodes.NodeState
import zio.keeper.membership.swim.{ Message, Nodes, Protocol }
import zio.logging.Logging.Logging
import zio.logging._
import zio.stream.ZStream

sealed trait Initial

object Initial {

  implicit def taggedRequests(
    implicit
    c4: ByteCodec[Join],
    c6: ByteCodec[Accept.type],
    c7: ByteCodec[Reject]
  ): TaggedCodec[Initial] =
    TaggedCodec.instance(
      {
        case _: Join   => 13
        case Accept    => 15
        case _: Reject => 16
      }, {
        case 13 => c4.asInstanceOf[ByteCodec[Initial]]
        case 15 => c6.asInstanceOf[ByteCodec[Initial]]
        case 16 => c7.asInstanceOf[ByteCodec[Initial]]
      }
    )

  final case class Join(nodeAddress: NodeAddress) extends Initial

  implicit val codecJoin: ByteCodec[Join] =
    ByteCodec.fromReadWriter(macroRW[Join])

  case object Accept extends Initial

  implicit val codecAccept: ByteCodec[Accept.type] =
    ByteCodec.fromReadWriter(macroRW[Accept.type])

  final case class Reject(msg: String) extends Initial

  object Reject {

    implicit val codec: ByteCodec[Reject] =
      ByteCodec.fromReadWriter(macroRW[Reject])
  }

  def protocol(nodes: Nodes, local: NodeAddress) =
    ZIO.accessM[Discovery with Logging](
      env =>
        Protocol[Initial].make(
          {
            case Message.Direct(_, Join(addr)) if addr == local =>
              ZIO.succeed(Message.NoResponse)
            case Message.Direct(_, join @ Join(addr)) =>
              nodes
                .nodeState(addr)
                .as(Message.NoResponse)
                .orElse(
                  nodes.addNode(addr) *>
                    nodes
                      .changeNodeState(addr, NodeState.Healthy)
                      .as(Message.Batch[Initial](Message.Direct(addr, Accept), Message.Broadcast(join)))
                )
            case Message.Direct(sender, Accept) =>
              nodes.addNode(sender) *>
                nodes
                  .changeNodeState(sender, NodeState.Healthy) *>
                Message.noResponse
            case Message.Direct(sender, Reject(msg)) =>
              log.error("Rejected from cluster: " + msg) *>
                nodes.disconnect(sender) *>
                Message.noResponse
          },
          ZStream
            .fromIterator(
              env.get.discoverNodes
                .tap(otherNodes => log.info("Discovered other nodes: " + otherNodes))
                .map(_.iterator)
            )
            .mapM(
              node =>
                NodeAddress
                  .fromSocketAddress(node)
                  .map(
                    nodeAddress => Message.Direct(nodeAddress, Join(local))
                  )
            )
        )
    )

}
