package zio.keeper.membership.swim.protocols

import java.util.concurrent.TimeUnit

import upickle.default._
import zio.clock.{ Clock, currentTime }
import zio.duration._
import zio.keeper.membership.swim.{ GossipState, Nodes, Protocol }
import zio.keeper.{ ByteCodec, TaggedCodec }
import zio.stm.TMap
import zio.stream.ZStream
import zio.{ Ref, Schedule, ZIO }

sealed trait FailureDetection[+A]

object FailureDetection {

  implicit def tagged[A](
    implicit
    c1: ByteCodec[Ack[A]],
    c2: ByteCodec[Ping[A]],
    c3: ByteCodec[PingReq[A]]
  ): TaggedCodec[FailureDetection[A]] =
    TaggedCodec.instance(
      {
        case _: Ack[A]     => 10
        case _: Ping[A]    => 11
        case _: PingReq[A] => 12
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

  private case class _Ack[A](nodeId: A, timestamp: Long, onBehalf: Option[(A, Long)])

  def protocol[A](nodes: Nodes[A])(implicit codec: TaggedCodec[FailureDetection[A]]) =
    for {
      deps  <- ZIO.environment[Clock]
      acks  <- TMap.empty[Long, _Ack[A]].commit
      ackId <- Ref.make(0L)
    } yield {
      def ack(id: Long) =
        (acks.get(id) <*
          acks
            .delete(id)).commit

      def withAck(onBehalf: Option[(A, Long)], fn: Long => (A, FailureDetection[A])) =
        for {
          ackId      <- ackId.update(_ + 1)
          timestamp  <- currentTime(TimeUnit.MILLISECONDS)
          nodeAndMsg = fn(ackId)
          _          <- acks.put(ackId, _Ack(nodeAndMsg._1, timestamp, onBehalf)).commit
        } yield nodeAndMsg

      Protocol[A, FailureDetection[A]](
        {
          case (_, Ack(ackId, state)) =>
            nodes.updateState(state.asInstanceOf[GossipState[A]]) *>
              ack(ackId).map {
                case Some(_Ack(_, _, Some((node, originalAckId)))) =>
                  Some((node, Ack(originalAckId, state)))
                case _ =>
                  None
              }

          case (sender, Ping(ackId, state0)) =>
            nodes.updateState(state0.asInstanceOf[GossipState[A]]) *>
              nodes.currentState.map(state => Some((sender, Ack[A](ackId, state))))

          case (sender, PingReq(to, originalAck, state0)) =>
            nodes.updateState(state0.asInstanceOf[GossipState[A]]) *>
              nodes.currentState
                .flatMap(state => withAck(Some((sender, originalAck)), ackId => (to, Ping(ackId, state))))
                .map(Some(_))
                .provide(deps)

        },
        ZStream
          .fromIterator(
            acks.toList.commit.map(_.iterator)
          )
          .zip(ZStream.repeatEffectWith(currentTime(TimeUnit.MICROSECONDS), Schedule.spaced(1.second)))
          .collectM {
            case ((ackId, ack), current) if current - ack.timestamp > 5.seconds.toMillis =>
              nodes.next
                .zip(nodes.currentState)
                .map {
                  case (node, state) =>
                    node.map(next => (next, PingReq(ack.nodeId, ackId, state)))
                }
          }
          .collect {
            case Some(req) => req
          }
          .merge(
            ZStream
              .repeatEffectWith(
                nodes.next <*> nodes.currentState,
                Schedule.spaced(3.seconds)
              )
              .collectM {
                case (Some(next), state) =>
                  withAck(None, ackId => (next, Ping(ackId, state)))
              }
          )
          .provide(deps)
      )
    }

}
