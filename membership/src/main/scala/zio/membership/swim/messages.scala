package zio.membership.swim

import upickle.default._
import zio.membership.{ByteCodec, TaggedCodec}


sealed trait SwimProtocol[+A]

object SwimProtocol {

  implicit def tagged[A](
                          implicit
                          c1: ByteCodec[Ack[A]],
                          c2: ByteCodec[Ping[A]],
                          c3: ByteCodec[PingReq[A]],
                          c4: ByteCodec[NewConnection[A]],
                          c5: ByteCodec[JoinCluster[A]]
                        ): TaggedCodec[SwimProtocol[A]] =
    TaggedCodec.instance(
      {
        case _: Ack[A]  => 10
        case _: Ping[A] => 11
        case _: PingReq[A]     => 12
        case _: NewConnection[A]    => 13
        case _: JoinCluster[A]    => 14
      }, {
        case 10 => c1.asInstanceOf[ByteCodec[SwimProtocol[A]]]
        case 11 => c2.asInstanceOf[ByteCodec[SwimProtocol[A]]]
        case 12 => c3.asInstanceOf[ByteCodec[SwimProtocol[A]]]
        case 13 => c4.asInstanceOf[ByteCodec[SwimProtocol[A]]]
        case 14 => c5.asInstanceOf[ByteCodec[SwimProtocol[A]]]
      }
    )

  final case class Ack[A](conversation: Long, state: GossipState[A]) extends SwimProtocol[A]

  object Ack {
    implicit def codec[A: ReadWriter]: ByteCodec[Ack[A]] =
      ByteCodec.fromReadWriter(macroRW[Ack[A]])

  }

  final case class Ping[A](ackConversation: Long, state: GossipState[A]) extends SwimProtocol[A]

  object Ping {
    implicit def codec[A: ReadWriter]: ByteCodec[Ping[A]] =
      ByteCodec.fromReadWriter(macroRW[Ping[A]])
  }

  final case class PingReq[A](target: A, ackConversation: Long, state: GossipState[A]) extends SwimProtocol[A]

  object PingReq {
    implicit def codec[A: ReadWriter]: ByteCodec[PingReq[A]] =
      ByteCodec.fromReadWriter(macroRW[PingReq[A]])
  }

  final case class NewConnection[A](state: GossipState[A], address: A) extends SwimProtocol[A]

  object NewConnection {
    implicit def codec[A: ReadWriter]: ByteCodec[NewConnection[A]] =
      ByteCodec.fromReadWriter(macroRW[NewConnection[A]])
  }

  final case class JoinCluster[A](state: GossipState[A], address: A) extends SwimProtocol[A]

  object JoinCluster {
    implicit def codec[A: ReadWriter]: ByteCodec[JoinCluster[A]] =
      ByteCodec.fromReadWriter(macroRW[JoinCluster[A]])
  }
}
