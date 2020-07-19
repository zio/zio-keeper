package zio.keeper.swim.protocols

import upickle.default._
import zio.{ ZIO, keeper }
import zio.keeper.discovery._
import zio.keeper.{ ByteCodec, NodeAddress }
import zio.keeper.swim.Nodes.{ NodeState, _ }
import zio.keeper.swim.{ ConversationId, Message, Nodes, Protocol }
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

  type Env = ConversationId with Nodes with Logging with Discovery

  def protocol(local: NodeAddress): ZIO[Env, keeper.Error, Protocol[Initial]] =
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
        .fromIterableM(discoverNodes.tap(otherNodes => log.info("Discovered other nodes: " + otherNodes)))
        .mapM { node =>
          NodeAddress
            .fromSocketAddress(node)
            .flatMap(nodeAddress => Message.direct(nodeAddress, Join(local)))
        }
    )

}
