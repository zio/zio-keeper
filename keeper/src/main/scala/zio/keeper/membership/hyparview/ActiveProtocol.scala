package zio.keeper.membership.hyparview

import java.util.UUID

import upickle.default._
import zio.Chunk
import zio.keeper.{ ByteCodec, NodeAddress, TaggedCodec }
import zio.keeper.orphans._

sealed abstract class ActiveProtocol

object ActiveProtocol {

  implicit def tagged(
    implicit
    c1: ByteCodec[Disconnect],
    c2: ByteCodec[ForwardJoin],
    c3: ByteCodec[Shuffle],
    c4: ByteCodec[Prune.type],
    c5: ByteCodec[IHave],
    c6: ByteCodec[Graft],
    c7: ByteCodec[UserMessage],
    c8: ByteCodec[Gossip]
  ): TaggedCodec[ActiveProtocol] =
    TaggedCodec.instance(
      {
        case _: Disconnect  => 10
        case _: ForwardJoin => 11
        case _: Shuffle     => 12
        case Prune          => 13
        case _: IHave       => 14
        case _: Graft       => 15
        case _: UserMessage => 16
        case _: Gossip      => 17
      }, {
        case 10 => c1.asInstanceOf[ByteCodec[ActiveProtocol]]
        case 11 => c2.asInstanceOf[ByteCodec[ActiveProtocol]]
        case 12 => c3.asInstanceOf[ByteCodec[ActiveProtocol]]
        case 13 => c4.asInstanceOf[ByteCodec[ActiveProtocol]]
        case 14 => c5.asInstanceOf[ByteCodec[ActiveProtocol]]
        case 15 => c6.asInstanceOf[ByteCodec[ActiveProtocol]]
        case 16 => c7.asInstanceOf[ByteCodec[ActiveProtocol]]
        case 17 => c8.asInstanceOf[ByteCodec[ActiveProtocol]]
      }
    )

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
    uuids: ::[UUID]
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
    payload: Chunk[Byte]
  ) extends PlumTreeProtocol

  object Gossip {

    implicit val codec: ByteCodec[Gossip] =
      ByteCodec.fromReadWriter(macroRW[Gossip])
  }
}
