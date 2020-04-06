package zio.membership.hyparview

import java.util.UUID

import upickle.default._
import zio.Chunk
import zio.keeper.membership.ByteCodec
import zio.keeper.membership.TaggedCodec
import zio.membership.orphans._

sealed abstract class ActiveProtocol[+T]

object ActiveProtocol {

  implicit def tagged[T](
    implicit
    c1: ByteCodec[Disconnect[T]],
    c2: ByteCodec[ForwardJoin[T]],
    c3: ByteCodec[Shuffle[T]],
    c4: ByteCodec[Prune.type],
    c5: ByteCodec[IHave],
    c6: ByteCodec[Graft],
    c7: ByteCodec[UserMessage],
    c8: ByteCodec[Gossip]
  ): TaggedCodec[ActiveProtocol[T]] =
    TaggedCodec.instance(
      {
        case _: Disconnect[T]  => 10
        case _: ForwardJoin[T] => 11
        case _: Shuffle[T]     => 12
        case Prune             => 13
        case _: IHave          => 14
        case _: Graft          => 15
        case _: UserMessage    => 16
        case _: Gossip         => 17
      }, {
        case 10 => c1.asInstanceOf[ByteCodec[ActiveProtocol[T]]]
        case 11 => c2.asInstanceOf[ByteCodec[ActiveProtocol[T]]]
        case 12 => c3.asInstanceOf[ByteCodec[ActiveProtocol[T]]]
        case 13 => c4.asInstanceOf[ByteCodec[ActiveProtocol[T]]]
        case 14 => c5.asInstanceOf[ByteCodec[ActiveProtocol[T]]]
        case 15 => c6.asInstanceOf[ByteCodec[ActiveProtocol[T]]]
        case 16 => c7.asInstanceOf[ByteCodec[ActiveProtocol[T]]]
        case 17 => c8.asInstanceOf[ByteCodec[ActiveProtocol[T]]]
      }
    )

  final case class Disconnect[T](
    sender: T,
    alive: Boolean
  ) extends ActiveProtocol[T]

  object Disconnect {

    implicit def codec[T: ReadWriter]: ByteCodec[Disconnect[T]] =
      ByteCodec.fromReadWriter(macroRW[Disconnect[T]])
  }

  final case class ForwardJoin[T](
    sender: T,
    originalSender: T,
    ttl: TimeToLive
  ) extends ActiveProtocol[T]

  object ForwardJoin {

    implicit def codec[T: ReadWriter]: ByteCodec[ForwardJoin[T]] =
      ByteCodec.fromReadWriter(macroRW[ForwardJoin[T]])
  }

  final case class Shuffle[T](
    sender: T,
    originalSender: T,
    activeNodes: List[T],
    passiveNodes: List[T],
    ttl: TimeToLive
  ) extends ActiveProtocol[T]

  object Shuffle {

    implicit def codec[T: ReadWriter]: ByteCodec[Shuffle[T]] =
      ByteCodec.fromReadWriter(macroRW[Shuffle[T]])
  }

  // Plumtree

  sealed abstract class PlumTreeProtocol extends ActiveProtocol[Nothing]

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
