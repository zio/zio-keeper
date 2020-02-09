package zio.keeper.membership.swim.protocols

import upickle.default._
import zio.ZIO
import zio.keeper.{ByteCodec, TaggedCodec}
import zio.keeper.membership.swim.{FailureDetectionProtocol, Gossip, GossipState, Protocol}

sealed trait FailureDetection[+A]

object FailureDetection {

  implicit def tagged[A](
                          implicit
                          c1: ByteCodec[Ack[A]],
                          c2: ByteCodec[Ping[A]],
                          c3: ByteCodec[PingReq[A]],
                        ): TaggedCodec[FailureDetection[A]] =
    TaggedCodec.instance(
      {
        case _: Ack[A]  => 10
        case _: Ping[A] => 11
        case _: PingReq[A]     => 12
      }, {
        case 10 => c1.asInstanceOf[ByteCodec[FailureDetection[A]]]
        case 11 => c2.asInstanceOf[ByteCodec[FailureDetection[A]]]
        case 12 => c3.asInstanceOf[ByteCodec[FailureDetection[A]]]
      }
    )

  final case class Ack[A](conversation: Long, state: GossipState[A]) extends FailureDetection[A]

  object Ack {
    implicit def codec[A: ReadWriter]: ByteCodec[Ack[A]] =
      ByteCodec.fromReadWriter(macroRW[Ack[A]])

  }

  final case class Ping[A](ackConversation: Long, state: GossipState[A]) extends FailureDetection[A]

  object Ping {
    implicit def codec[A: ReadWriter]: ByteCodec[Ping[A]] =
      ByteCodec.fromReadWriter(macroRW[Ping[A]])
  }

  final case class PingReq[A](target: A, ackConversation: Long, state: GossipState[A]) extends FailureDetection[A]

  object PingReq {
    implicit def codec[A: ReadWriter]: ByteCodec[PingReq[A]] =
      ByteCodec.fromReadWriter(macroRW[PingReq[A]])
  }

  def protocol[A](implicit tg: TaggedCodec[FailureDetectionProtocol[A]]) =
    Protocol[Gossip[A],FailureDetectionProtocol[A]] {
      case FailureDetectionProtocol.Ack(ackId, state) =>
        ZIO.access[Gossip[A]](_.gossip).flatMap(gossip =>
          gossip.updateState(state.asInstanceOf[GossipState[A]]) *>
          gossip.ack(ackId).as(
            Protocol.noReply
          )
        )
    }

}
