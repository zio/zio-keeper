package zio.keeper.membership.swim.protocols

import upickle.default._
import zio.keeper.discovery._
import zio.keeper.membership.swim.Nodes.{ NodeState, _ }
import zio.keeper.membership.swim.{ Message, Protocol }
import zio.keeper.{ ByteCodec, NodeAddress }
import zio.logging._
import zio.stream.ZStream

sealed trait Initial

object Initial {

  final case class Join(nodeAddress: NodeAddress) extends Initial
  case object Accept                              extends Initial
  final case class Reject(msg: String)            extends Initial

  implicit val joinCodec: ByteCodec[Join] =
    ByteCodec.fromReadWriter(macroRW[Join])

  implicit val acceptCodec: ByteCodec[Accept.type] =
    ByteCodec.fromReadWriter(macroRW[Accept.type])

  implicit val rejectCodec: ByteCodec[Reject] =
    ByteCodec.fromReadWriter(macroRW[Reject])

  implicit val byteCodec: ByteCodec[Initial] =
    ByteCodec.tagged[Initial][
      Join,
      Accept.type,
      Reject
    ]

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
