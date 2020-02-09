package zio.membership.hyparview

import upickle.default._
import zio.Chunk
import zio.membership.{ByteCodec, TaggedCodec}


sealed trait ActiveProtocol[+T]

object ActiveProtocol {

  implicit def tagged[T](
    implicit
    c1: ByteCodec[Disconnect[T]],
    c2: ByteCodec[ForwardJoin[T]],
    c3: ByteCodec[Shuffle[T]],
    c4: ByteCodec[UserMessage]
  ): TaggedCodec[ActiveProtocol[T]] =
    TaggedCodec.instance(
      {
        case _: Disconnect[T]  => 10
        case _: ForwardJoin[T] => 11
        case _: Shuffle[T]     => 12
        case _: UserMessage    => 13
      }, {
        case 10 => c1.asInstanceOf[ByteCodec[ActiveProtocol[T]]]
        case 11 => c2.asInstanceOf[ByteCodec[ActiveProtocol[T]]]
        case 12 => c3.asInstanceOf[ByteCodec[ActiveProtocol[T]]]
        case 13 => c4.asInstanceOf[ByteCodec[ActiveProtocol[T]]]
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

  final case class UserMessage(
    msg: Chunk[Byte]
  ) extends ActiveProtocol[Nothing]

  object UserMessage {

    implicit val codec: ByteCodec[UserMessage] =
      ByteCodec.fromReadWriter(
        implicitly[ReadWriter[Array[Byte]]].bimap[UserMessage](_.msg.toArray, bA => UserMessage(Chunk.fromArray(bA)))
      )
  }
}

sealed trait InitialProtocol[T]

object InitialProtocol {

  implicit def tagged[T](
    implicit
    c1: ByteCodec[Neighbor[T]],
    c2: ByteCodec[InitialMessage.Join[T]],
    c3: ByteCodec[InitialMessage.ForwardJoinReply[T]],
    c4: ByteCodec[InitialMessage.ShuffleReply[T]]
  ): TaggedCodec[InitialProtocol[T]] =
    TaggedCodec.instance(
      {
        case _: Neighbor[T]                        => 0
        case _: InitialMessage.Join[T]             => 1
        case _: InitialMessage.ForwardJoinReply[T] => 2
        case _: InitialMessage.ShuffleReply[T]     => 3
      }, {
        case 0 => c1.asInstanceOf[ByteCodec[InitialProtocol[T]]]
        case 1 => c2.asInstanceOf[ByteCodec[InitialProtocol[T]]]
        case 2 => c3.asInstanceOf[ByteCodec[InitialProtocol[T]]]
        case 3 => c4.asInstanceOf[ByteCodec[InitialProtocol[T]]]
      }
    )
}

final case class Neighbor[T](
  sender: T,
  isHighPriority: Boolean
) extends InitialProtocol[T]

object Neighbor {

  implicit def codec[T: ReadWriter]: ByteCodec[Neighbor[T]] =
    ByteCodec.fromReadWriter(macroRW[Neighbor[T]])
}

sealed trait InitialMessage[T] extends InitialProtocol[T]

object InitialMessage {

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

final case class JoinReply[T](
  remote: T
)

object JoinReply {

  implicit def codec[T: ReadWriter]: ByteCodec[JoinReply[T]] =
    ByteCodec.fromReadWriter(macroRW[JoinReply[T]])

}

sealed trait NeighborReply

object NeighborReply {

  implicit val tagged: TaggedCodec[NeighborReply] =
    TaggedCodec.instance(
      {
        case _: Reject.type => 20
        case _: Accept.type => 21
      }, {
        case 20 => ByteCodec[Reject.type].asInstanceOf[ByteCodec[NeighborReply]]
        case 21 => ByteCodec[Accept.type].asInstanceOf[ByteCodec[NeighborReply]]
      }
    )

  case object Reject extends NeighborReply {

    implicit val codec: ByteCodec[Reject.type] =
      ByteCodec.fromReadWriter(macroRW[Reject.type])
  }

  case object Accept extends NeighborReply {

    implicit val codec: ByteCodec[Accept.type] =
      ByteCodec.fromReadWriter(macroRW[Accept.type])
  }
}
