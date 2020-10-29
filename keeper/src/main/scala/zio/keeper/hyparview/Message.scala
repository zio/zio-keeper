package zio.keeper.hyparview

import upickle.default._
import zio.Chunk
import zio.keeper.{ ByteCodec, NodeAddress }

import java.util.UUID

sealed abstract class Message

object Message {

  implicit val codec: ByteCodec[Message] =
    ByteCodec.tagged[Message][
      Disconnect,
      ForwardJoin,
      ForwardJoinReply,
      Gossip,
      Graft,
      IHave,
      Join,
      Neighbor,
      NeighborAccept.type,
      NeighborReject.type,
      Prune.type,
      Shuffle,
      ShuffleReply,
      UserMessage
    ]

  final case class Join(
    sender: NodeAddress
  ) extends Message

  object Join {

    implicit val codec: ByteCodec[Join] =
      ByteCodec.fromReadWriter(macroRW[Join])
  }

  case object NeighborReject extends Message {

    implicit val codec: ByteCodec[NeighborReject.type] =
      ByteCodec.fromReadWriter(macroRW[NeighborReject.type])

  }

  case object NeighborAccept extends Message {

    implicit val codec: ByteCodec[NeighborAccept.type] =
      ByteCodec.fromReadWriter(macroRW[NeighborAccept.type])

  }

  final case class Disconnect(
    alive: Boolean
  ) extends Message

  object Disconnect {

    implicit val codec: ByteCodec[Disconnect] =
      ByteCodec.fromReadWriter(macroRW[Disconnect])
  }

  /**
   * Messages that can be sent to nodes in the active and passive views.
   */
  sealed abstract class ViewMessage extends Message

  final case class ForwardJoin(
    originalSender: NodeAddress,
    ttl: TimeToLive
  ) extends ViewMessage

  object ForwardJoin {

    implicit val codec: ByteCodec[ForwardJoin] =
      ByteCodec.fromReadWriter(macroRW[ForwardJoin])
  }

  final case class ForwardJoinReply(
    sender: NodeAddress
  ) extends ViewMessage

  object ForwardJoinReply {

    implicit val codec: ByteCodec[ForwardJoinReply] =
      ByteCodec.fromReadWriter(macroRW[ForwardJoinReply])
  }

  final case class Neighbor(
    sender: NodeAddress,
    isHighPriority: Boolean
  ) extends ViewMessage

  object Neighbor {

    implicit val codec: ByteCodec[Neighbor] =
      ByteCodec.fromReadWriter(macroRW[Neighbor])

  }

  final case class Shuffle(
    sender: NodeAddress,
    originalSender: NodeAddress,
    activeNodes: List[NodeAddress],
    passiveNodes: List[NodeAddress],
    ttl: TimeToLive
  ) extends ViewMessage

  object Shuffle {

    implicit val codec: ByteCodec[Shuffle] =
      ByteCodec.fromReadWriter(macroRW[Shuffle])
  }

  final case class ShuffleReply(
    passiveNodes: List[NodeAddress],
    sentOriginally: List[NodeAddress]
  ) extends ViewMessage

  object ShuffleReply {

    implicit val codec: ByteCodec[ShuffleReply] =
      ByteCodec.fromReadWriter(macroRW[ShuffleReply])
  }

  /**
   * Messages used by plumtree layer.
   */
  sealed abstract class PeerMessage extends ViewMessage

  case object Prune extends PeerMessage {

    implicit val codec: ByteCodec[Prune.type] =
      ByteCodec.fromReadWriter(macroRW[Prune.type])
  }

  final case class IHave(
    messages: Chunk[(UUID, Round)]
  ) extends PeerMessage

  object IHave {

    implicit val codec: ByteCodec[IHave] =
      ByteCodec[Chunk[(UUID, Round)]].bimap(IHave.apply, _.messages)
  }

  final case class Graft(
    uuid: UUID
  ) extends PeerMessage

  object Graft {

    implicit val codec: ByteCodec[Graft] =
      ByteCodec.fromReadWriter(macroRW[Graft])
  }

  final case class Gossip(
    uuid: UUID,
    payload: Chunk[Byte],
    round: Round
  ) extends PeerMessage

  object Gossip {

    implicit val codec: ByteCodec[Gossip] =
      ByteCodec[(UUID, (Chunk[Byte], Round))].bimap(
        { case (uuid, (payload, round)) => Gossip(uuid, payload, round) },
        gossip => (gossip.uuid, (gossip.payload, gossip.round))
      )
  }

  final case class UserMessage(
    payload: Chunk[Byte]
  ) extends PeerMessage

  object UserMessage {

    implicit val codec: ByteCodec[UserMessage] =
      ByteCodec[Chunk[Byte]].bimap(UserMessage.apply, _.payload)
  }

}
