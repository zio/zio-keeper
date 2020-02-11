package zio.membership.swim

import upickle.default._
import zio.membership.{ByteCodec, Member, TaggedCodec}

sealed trait FailureDetectionProtocol[+A]

object FailureDetectionProtocol {

  implicit def tagged[A](
                          implicit
                          c1: ByteCodec[Ack[A]],
                          c2: ByteCodec[Ping[A]],
                          c3: ByteCodec[PingReq[A]],
                        ): TaggedCodec[FailureDetectionProtocol[A]] =
    TaggedCodec.instance(
      {
        case _: Ack[A]  => 10
        case _: Ping[A] => 11
        case _: PingReq[A]     => 12
      }, {
        case 10 => c1.asInstanceOf[ByteCodec[FailureDetectionProtocol[A]]]
        case 11 => c2.asInstanceOf[ByteCodec[FailureDetectionProtocol[A]]]
        case 12 => c3.asInstanceOf[ByteCodec[FailureDetectionProtocol[A]]]
      }
    )

  final case class Ack[A](conversation: Long, state: GossipState[A]) extends FailureDetectionProtocol[A]

  object Ack {
    implicit def codec[A: ReadWriter]: ByteCodec[Ack[A]] =
      ByteCodec.fromReadWriter(macroRW[Ack[A]])

  }

  final case class Ping[A](ackConversation: Long, state: GossipState[A]) extends FailureDetectionProtocol[A]

  object Ping {
    implicit def codec[A: ReadWriter]: ByteCodec[Ping[A]] =
      ByteCodec.fromReadWriter(macroRW[Ping[A]])
  }

  final case class PingReq[A](target: A, ackConversation: Long, state: GossipState[A]) extends FailureDetectionProtocol[A]

  object PingReq {
    implicit def codec[A: ReadWriter]: ByteCodec[PingReq[A]] =
      ByteCodec.fromReadWriter(macroRW[PingReq[A]])
  }

}

sealed trait SuspicionProtocol[A]

object SuspicionProtocol {
  final case class Suspect[A](member: Member[A]) extends SuspicionProtocol[A]
  final case class Confirm[A](member: Member[A]) extends SuspicionProtocol[A]
  final case class Alive[A](member: Member[A]) extends SuspicionProtocol[A]
}

sealed trait InitialProtocol[A]

object InitialProtocol {

  sealed trait Reply[A]

  implicit def taggedRequests[A](
                          implicit
                          c4: ByteCodec[Join[A]],
                          c5: ByteCodec[JoinCluster[A]]
                        ): TaggedCodec[InitialProtocol[A]] =
    TaggedCodec.instance(
      {
        case _: Join[A]    => 13
        case _: JoinCluster[A]    => 14
      }, {
        case 13 => c4.asInstanceOf[ByteCodec[InitialProtocol[A]]]
        case 14 => c5.asInstanceOf[ByteCodec[InitialProtocol[A]]]
      }
    )

  final case class Join[A](state: GossipState[A], address: A) extends InitialProtocol[A]

  object Join {
    implicit def codec[A: ReadWriter]: ByteCodec[Join[A]] =
      ByteCodec.fromReadWriter(macroRW[Join[A]])

    case class Accept[A](state: GossipState[A], address: A) extends Reply[A]
    case class Reject[A](msg: String) extends Reply[A]
  }

  final case class JoinCluster[A](state: GossipState[A], address: A) extends InitialProtocol[A]

  object JoinCluster {
    implicit def codec[A: ReadWriter]: ByteCodec[JoinCluster[A]] =
      ByteCodec.fromReadWriter(macroRW[JoinCluster[A]])
  }

}