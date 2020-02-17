package zio.keeper.membership.swim.protocols

import upickle.default._
import zio.duration._
import zio.keeper.membership.NodeAddress
import zio.keeper.membership.swim.{ GossipState, Nodes, Protocol }
import zio.keeper.{ ByteCodec, TaggedCodec }
import zio.logging.slf4j._
import zio.stm.TMap
import zio.stream.ZStream
import zio.{ Ref, Schedule, ZIO }

sealed trait FailureDetection

object FailureDetection {

  implicit def tagged(
    implicit
    c1: ByteCodec[Ack],
    c2: ByteCodec[Ping],
    c3: ByteCodec[PingReq]
  ): TaggedCodec[FailureDetection] =
    TaggedCodec.instance(
      {
        case _: Ack     => 10
        case _: Ping    => 11
        case _: PingReq => 12
      }, {
        case 10 => c1.asInstanceOf[ByteCodec[FailureDetection]]
        case 11 => c2.asInstanceOf[ByteCodec[FailureDetection]]
        case 12 => c3.asInstanceOf[ByteCodec[FailureDetection]]
      }
    )

  final case class Ack(conversation: Long, state: GossipState) extends FailureDetection

  object Ack {

    implicit val codec: ByteCodec[Ack] =
      ByteCodec.fromReadWriter(macroRW[Ack])

  }

  final case class Ping(ackConversation: Long, state: GossipState) extends FailureDetection

  object Ping {

    implicit val codec: ByteCodec[Ping] =
      ByteCodec.fromReadWriter(macroRW[Ping])
  }

  final case class PingReq(target: NodeAddress, ackConversation: Long, state: GossipState) extends FailureDetection

  object PingReq {

    implicit val codec: ByteCodec[PingReq] =
      ByteCodec.fromReadWriter(macroRW[PingReq])
  }

  private case class _Ack(nodeId: NodeAddress, onBehalf: Option[(NodeAddress, Long)])

  def protocol(nodes: Nodes, protocolPeriod: Duration) =
    for {
      acks  <- TMap.empty[Long, _Ack].commit
      ackId <- Ref.make(0L)
      protocol <- {

        def ack(id: Long) =
          (acks.get(id) <*
            acks
              .delete(id)).commit

        def withAck(onBehalf: Option[(NodeAddress, Long)], fn: Long => (NodeAddress, FailureDetection)) =
          for {
            ackId      <- ackId.update(_ + 1)
            nodeAndMsg = fn(ackId)
            _          <- acks.put(ackId, _Ack(nodeAndMsg._1, onBehalf)).commit
          } yield nodeAndMsg

        Protocol[NodeAddress, FailureDetection].apply(
          {
            case (_, Ack(ackId, state)) =>
              logger.info("receive ack")
              nodes.updateState(state.asInstanceOf[GossipState]) *>
                ack(ackId).map {
                  case Some(_Ack(_, Some((node, originalAckId)))) =>
                    Some((node, Ack(originalAckId, state)))
                  case _ =>
                    None
                }

            case (sender, Ping(ackId, state0)) =>
              nodes.updateState(state0.asInstanceOf[GossipState]) *>
                logger.info("Send Ack " + ackId + " to: " + sender)
              nodes.currentState.map(state => Some((sender, Ack(ackId, state))))

            case (sender, PingReq(to, originalAck, state0)) =>
              nodes.updateState(state0.asInstanceOf[GossipState]) *>
                nodes.currentState
                  .flatMap(state => withAck(Some((sender, originalAck)), ackId => (to, Ping(ackId, state))))
                  .map(Some(_))

          },
          ZStream
            .repeatEffectWith(
              ZIO.unit,
              Schedule.spaced(protocolPeriod)
            )
            *>
              ZStream
                .repeatEffect(nodes.next <*> nodes.currentState)
                .take(1)
                .collectM {
                  case (Some(next), state) =>
                    logger.info("Send Ping to: " + next) *>
                      withAck(None, ackId => (next, Ping(ackId, state)))
                }
                .merge(
                  // Every protocol round check for outstanding acks
                  ZStream
                    .fromIterator(
                      acks.toList.commit.map(_.iterator)
                    )
                    .collectM {
                      case (ackId, ack) =>
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
                )
        )
      }

    } yield protocol

}
