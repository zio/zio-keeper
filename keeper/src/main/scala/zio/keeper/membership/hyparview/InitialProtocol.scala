package zio.keeper.membership.hyparview

import upickle.default._
import zio.keeper.{ ByteCodec, NodeAddress, TaggedCodec }

sealed abstract class InitialProtocol

object InitialProtocol {

  implicit def tagged(
    implicit
    c1: ByteCodec[Neighbor],
    c2: ByteCodec[Join],
    c3: ByteCodec[ForwardJoinReply],
    c4: ByteCodec[ShuffleReply]
  ): TaggedCodec[InitialProtocol] =
    TaggedCodec.instance(
      {
        case _: Neighbor         => 0
        case _: Join             => 1
        case _: ForwardJoinReply => 2
        case _: ShuffleReply     => 3
      }, {
        case 0 => c1.asInstanceOf[ByteCodec[InitialProtocol]]
        case 1 => c2.asInstanceOf[ByteCodec[InitialProtocol]]
        case 2 => c3.asInstanceOf[ByteCodec[InitialProtocol]]
        case 3 => c4.asInstanceOf[ByteCodec[InitialProtocol]]
      }
    )

  final case class Neighbor(
    sender: NodeAddress,
    isHighPriority: Boolean
  ) extends InitialProtocol

  object Neighbor {

    implicit val codec: ByteCodec[Neighbor] =
      ByteCodec.fromReadWriter(macroRW[Neighbor])

  }

  // messages send to active nodes
  sealed abstract class InitialMessage extends InitialProtocol

  final case class Join(
    sender: NodeAddress
  ) extends InitialMessage

  object Join {

    implicit val codec: ByteCodec[Join] =
      ByteCodec.fromReadWriter(macroRW[Join])
  }

  final case class ForwardJoinReply(
    sender: NodeAddress
  ) extends InitialMessage

  object ForwardJoinReply {

    implicit val codec: ByteCodec[ForwardJoinReply] =
      ByteCodec.fromReadWriter(macroRW[ForwardJoinReply])
  }

  final case class ShuffleReply(
    passiveNodes: List[NodeAddress],
    sentOriginally: List[NodeAddress]
  ) extends InitialMessage

  object ShuffleReply {

    implicit val codec: ByteCodec[ShuffleReply] =
      ByteCodec.fromReadWriter(macroRW[ShuffleReply])
  }
}
