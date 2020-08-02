package zio.keeper.hyparview

import upickle.default._
import zio.keeper.{ ByteCodec, NodeAddress }

sealed abstract class InitialProtocol

object InitialProtocol {

  implicit val byteCodec: ByteCodec[InitialProtocol] =
    ByteCodec.tagged[InitialProtocol][
      Neighbor,
      Join,
      ForwardJoinReply,
      ShuffleReply
    ]

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

  sealed abstract class Reply

  object Reply {

    final case class JoinReply(
      remote: NodeAddress
    ) extends Reply

    object JoinReply {

      implicit val codec: ByteCodec[JoinReply] =
        ByteCodec.fromReadWriter(macroRW[JoinReply])

    }

    case object NeighborReject extends Reply {

      implicit val codec: ByteCodec[NeighborReject.type] =
        ByteCodec.fromReadWriter(macroRW[NeighborReject.type])

    }

    case object NeighborAccept extends Reply {

      implicit val codec: ByteCodec[NeighborAccept.type] =
        ByteCodec.fromReadWriter(macroRW[NeighborAccept.type])

    }

    implicit val codec: ByteCodec[Reply] =
      ByteCodec.tagged[Reply][
        JoinReply,
        NeighborReject.type,
        NeighborAccept.type
      ]
  }
}
