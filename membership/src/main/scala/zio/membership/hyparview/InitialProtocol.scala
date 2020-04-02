package zio.membership.hyparview

import upickle.default._
import zio.keeper.membership.ByteCodec
import zio.keeper.membership.TaggedCodec

sealed abstract class InitialProtocol[T]

object InitialProtocol {

  implicit def tagged[T](
    implicit
    c1: ByteCodec[Neighbor[T]],
    c2: ByteCodec[Join[T]],
    c3: ByteCodec[ForwardJoinReply[T]],
    c4: ByteCodec[ShuffleReply[T]]
  ): TaggedCodec[InitialProtocol[T]] =
    TaggedCodec.instance(
      {
        case _: Neighbor[T]         => 0
        case _: Join[T]             => 1
        case _: ForwardJoinReply[T] => 2
        case _: ShuffleReply[T]     => 3
      }, {
        case 0 => c1.asInstanceOf[ByteCodec[InitialProtocol[T]]]
        case 1 => c2.asInstanceOf[ByteCodec[InitialProtocol[T]]]
        case 2 => c3.asInstanceOf[ByteCodec[InitialProtocol[T]]]
        case 3 => c4.asInstanceOf[ByteCodec[InitialProtocol[T]]]
      }
    )

  final case class Neighbor[T](
    sender: T,
    isHighPriority: Boolean
  ) extends InitialProtocol[T]

  object Neighbor {

    implicit def codec[T: ReadWriter]: ByteCodec[Neighbor[T]] =
      ByteCodec.fromReadWriter(macroRW[Neighbor[T]])

  }

  // messages send to active nodes
  sealed abstract class InitialMessage[T] extends InitialProtocol[T]

  final case class Join[T](
    sender: T
  ) extends InitialMessage[T]

  object Join {

    implicit def codec[T: ReadWriter]: ByteCodec[Join[T]] =
      ByteCodec.fromReadWriter(macroRW[Join[T]])
  }

  final case class ForwardJoinReply[T](
    sender: T
  ) extends InitialMessage[T]

  object ForwardJoinReply {

    implicit def codec[T: ReadWriter]: ByteCodec[ForwardJoinReply[T]] =
      ByteCodec.fromReadWriter(macroRW[ForwardJoinReply[T]])
  }

  final case class ShuffleReply[T](
    passiveNodes: List[T],
    sentOriginally: List[T]
  ) extends InitialMessage[T]

  object ShuffleReply {

    implicit def codec[T: ReadWriter]: ByteCodec[ShuffleReply[T]] =
      ByteCodec.fromReadWriter(macroRW[ShuffleReply[T]])
  }
}
