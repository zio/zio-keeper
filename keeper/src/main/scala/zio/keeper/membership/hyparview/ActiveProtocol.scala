package zio.keeper.membership.hyparview

import java.util.UUID

import upickle.default._
import zio.Chunk
import zio.keeper.{ ByteCodec, NodeAddress }
import zio.keeper.orphans._

sealed abstract class ActiveProtocol

object ActiveProtocol {

  implicit val byteCodec: ByteCodec[ActiveProtocol] =
    ByteCodec.tagged[ActiveProtocol][
      Disconnect,
      ForwardJoin,
      Shuffle,
      Prune.type,
      IHave,
      Graft,
      UserMessage,
      Gossip
    ]

  final case class Disconnect(
    sender: NodeAddress,
    alive: Boolean
  ) extends ActiveProtocol

  object Disconnect {

    implicit val codec: ByteCodec[Disconnect] =
      ByteCodec.fromReadWriter(macroRW[Disconnect])
  }

  final case class ForwardJoin(
    sender: NodeAddress,
    originalSender: NodeAddress,
    ttl: TimeToLive
  ) extends ActiveProtocol

  object ForwardJoin {

    implicit val codec: ByteCodec[ForwardJoin] =
      ByteCodec.fromReadWriter(macroRW[ForwardJoin])
  }

  final case class Shuffle(
    sender: NodeAddress,
    originalSender: NodeAddress,
    activeNodes: List[NodeAddress],
    passiveNodes: List[NodeAddress],
    ttl: TimeToLive
  ) extends ActiveProtocol

  object Shuffle {

    implicit val codec: ByteCodec[Shuffle] =
      ByteCodec.fromReadWriter(macroRW[Shuffle])
  }

  // Plumtree

  sealed abstract class PlumTreeProtocol extends ActiveProtocol

  case object Prune extends PlumTreeProtocol {

    implicit val codec: ByteCodec[Prune.type] =
      ByteCodec.fromReadWriter(macroRW[Prune.type])
  }

  final case class IHave(
    messages: ::[(UUID, Int)] // TODO: switch to round type
  ) extends PlumTreeProtocol

  object IHave {

    implicit val codec: ByteCodec[IHave] =
      ByteCodec.fromReadWriter(macroRW[IHave])
  }

  final case class Graft(
    uuid: UUID
  ) extends PlumTreeProtocol

  object Graft {

    implicit val codec: ByteCodec[Graft] =
      ByteCodec.fromReadWriter(macroRW[Graft])
  }

  final case class UserMessage(
    payload: Chunk[Byte]
  ) extends PlumTreeProtocol

  object UserMessage {

    implicit val codec: ByteCodec[UserMessage] =
      ByteCodec.fromReadWriter(macroRW[UserMessage])
  }

  final case class Gossip(
    uuid: UUID,
    payload: Chunk[Byte],
    round: Int // TODO: switch to Round type
  ) extends PlumTreeProtocol

  object Gossip {

    implicit val codec: ByteCodec[Gossip] =
      ByteCodec.fromReadWriter(macroRW[Gossip])
  }
}
