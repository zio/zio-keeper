package zio.keeper.membership.swim.protocols

import upickle.default._
import zio.keeper.discovery._
import zio.keeper.membership.swim.Nodes._
import zio.keeper.membership.swim.{ Message, Protocol }
import zio.keeper.{ ByteCodec, NodeAddress, TaggedCodec }
import zio.ZIO
import zio.keeper.{ ByteCodec, NodeAddress }
import zio.keeper.discovery.Discovery
import zio.keeper.membership.swim.Nodes.NodeState
import zio.keeper.membership.swim.{ Message, Nodes, Protocol }
import zio.logging.Logging
import zio.logging._
import zio.stream.ZStream

sealed trait Initial

object Initial {

  implicit val byteCodec: ByteCodec[Initial] =
    ByteCodec.tagged[Initial][
      Join,
      Accept.type,
      Reject
    ]

  final case class Join(nodeAddress: NodeAddress) extends Initial

  object Join {

    implicit val codecJoin: ByteCodec[Join] =
      ByteCodec.fromReadWriter(macroRW[Join])
  }

  case object Accept extends Initial {

    implicit val codecAccept: ByteCodec[Accept.type] =
      ByteCodec.fromReadWriter(macroRW[Accept.type])
  }

  final case class Reject(msg: String) extends Initial

  object Reject {

    implicit val codec: ByteCodec[Reject] =
      ByteCodec.fromReadWriter(macroRW[Reject])
  }

  def protocol(local: NodeAddress) =
    Protocol[Initial].make(
      {
        case Message.Direct(_, _, Join(addr)) if addr == local =>
          Message.noResponse
        case Message.Direct(_, _, join @ Join(addr)) =>
          nodeState(addr)
            .as(Message.NoResponse)
            .orElse(
              addNode(addr) *>
                changeNodeState(addr, NodeState.Healthy) *>
                Message.direct(addr, Accept).map(accept => Message.Batch[Initial](accept, Message.Broadcast(join)))
            )
        case Message.Direct(sender, _, Accept) =>
          addNode(sender) *>
            changeNodeState(sender, NodeState.Healthy) *>
            Message.noResponse
        case Message.Direct(sender, _, Reject(msg)) =>
          log.error("Rejected from cluster: " + msg) *>
            disconnect(sender) *>
            Message.noResponse
      },
      ZStream
        .fromIterator(
          discoverNodes
            .tap(otherNodes => log.info("Discovered other nodes: " + otherNodes))
            .map(_.iterator)
        )
        .mapM(
          node =>
            NodeAddress
              .fromSocketAddress(node)
              .flatMap(
                nodeAddress => Message.direct(nodeAddress, Join(local))
              )
        )
    )

}
