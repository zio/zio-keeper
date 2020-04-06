package zio.keeper.membership.swim.protocols

import upickle.default._
import zio.ZIO
import zio.keeper.ClusterError.UnknownNode
import zio.keeper.discovery.Discovery
import zio.keeper.membership.NodeAddress
import zio.keeper.membership.swim.Nodes.NodeState
import zio.keeper.membership.swim.{ Message, Nodes, Protocol }
import zio.keeper.membership.{ ByteCodec, TaggedCodec }
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

  case class Reject(msg: String) extends Initial

  object Reject {

    implicit val codec: ByteCodec[Reject] =
      ByteCodec.fromReadWriter(macroRW[Reject])
  }

  def protocol(nodes: Nodes, local: NodeAddress) =
    ZIO.accessM[Discovery with Logging](
      env =>
        Protocol[Initial](
          {
            case Message.Direct(_, join@Join(addr)) =>
              nodes.addNode(addr) *>
              nodes
                .changeNodeState(addr, NodeState.Healthy)
                .as(Message.Batch[Initial](Message.Direct(addr, Accept), Message.Broadcast(join)))
//                .catchSome {
//                  //this handle Join messages that was piggybacked
//                  case UnknownNode(_) =>
//                    nodes.addNode(addr) *>
//                      nodes
//                        .changeNodeState(addr, NodeState.Healthy)
//                        .as(None)
//                }

            case Message.Direct(sender, Accept) =>
              nodes.addNode(sender) *>
              nodes
                .changeNodeState(sender, NodeState.Healthy)
                .as(Message.NoResponse)
            case Message.Direct(sender, Reject(msg)) =>
              log(LogLevel.Error)("Rejected from cluster: " + msg) *>
                nodes.disconnect(sender).as(Message.NoResponse)
          },
          ZStream
            .fromIterator(
              env.get.discoverNodes.map(_.iterator)
            )
            .mapM(
              node => NodeAddress(node).map(
                nodeAddress =>
                  Message.Direct(nodeAddress, Join(local))
              )
            )
        )
    )

}
