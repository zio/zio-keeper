package zio.keeper.membership.swim.protocols

import upickle.default._
import zio.duration._
import zio.keeper.membership.swim.Nodes.NodeState
import zio.keeper.membership.swim.{NodeId, Nodes, Protocol, Message}
import zio.keeper.{ByteCodec, TaggedCodec}
import zio.stm.TMap
import zio.stream.ZStream
import zio.{Ref, Schedule, ZIO}

sealed trait FailureDetection

object FailureDetection {

  implicit def tagged(
    implicit
    c1: ByteCodec[Ack],
    c2: ByteCodec[Ping],
    c3: ByteCodec[PingReq],
    c4: ByteCodec[Nack]
  ): TaggedCodec[FailureDetection] =
    TaggedCodec.instance(
      {
        case _: Ack     => 10
        case _: Ping    => 11
        case _: PingReq => 12
        case _: Nack => 13
      }, {
        case 10 => c1.asInstanceOf[ByteCodec[FailureDetection]]
        case 11 => c2.asInstanceOf[ByteCodec[FailureDetection]]
        case 12 => c3.asInstanceOf[ByteCodec[FailureDetection]]
        case 13 => c4.asInstanceOf[ByteCodec[FailureDetection]]
      }
    )

  final case class Ack(conversation: Long) extends FailureDetection

  object Ack {

    implicit val codec: ByteCodec[Ack] =
      ByteCodec.fromReadWriter(macroRW[Ack])

  }

  final case class Nack(conversation: Long) extends FailureDetection

  object Nack {

    implicit val codec: ByteCodec[Nack] =
      ByteCodec.fromReadWriter(macroRW[Nack])

  }

  final case class Ping(ackConversation: Long) extends FailureDetection

  object Ping {

    implicit val codec: ByteCodec[Ping] =
      ByteCodec.fromReadWriter(macroRW[Ping])
  }

  final case class PingReq(target: NodeId, ackConversation: Long) extends FailureDetection

  object PingReq {

    implicit val codec: ByteCodec[PingReq] =
      ByteCodec.fromReadWriter(macroRW[PingReq])
  }

  private case class _Ack(target: NodeId, onBehalf: Option[(NodeId, Long)])

  def protocol(nodes: Nodes, protocolPeriod: Duration) =
    for {
      acks  <- TMap.empty[Long, _Ack].commit
      ackId <- Ref.make(0L)
      protocol <- {

        def ack(id: Long) =
          (acks.get(id) <*
            acks
              .delete(id)).commit

        def withAck(onBehalf: Option[(NodeId, Long)], fn: Long => Message.Direct[FailureDetection]) =
          for {
            ackId      <- ackId.update(_ + 1)
            nodeAndMsg = fn(ackId)
            _          <- acks.put(ackId, _Ack(nodeAndMsg.nodeId, onBehalf)).commit
          } yield nodeAndMsg

        Protocol[NodeId, FailureDetection](
          {
            case (_, Ack(ackId)) =>
              ack(ackId).map {
                case Some(_Ack(_, Some((node, originalAckId)))) =>
                  Message.Direct(node, Ack(originalAckId))
                case _ =>
                  Message.Empty
              }

            case (sender, Ping(ackId)) =>
              ZIO.succeed(Message.Direct(sender, Ack(ackId)))

            case (sender, PingReq(to, originalAck)) =>
              withAck(Some((sender, originalAck)), ackId => Message.Direct(to, Ping(ackId)))
            case (_, Nack(_)) => ZIO.succeed(Message.Empty)

          },
          ZStream
            .repeatEffectWith(
              ZIO.unit,
              Schedule.spaced(protocolPeriod)
            )
            *>
              ZStream
                .fromEffect(nodes.next)
                .collectM {
                  case Some(next) =>
                    withAck(None, ackId => Message.Direct(next, Ping(ackId)))
                }
                .merge(
                  // Every protocol round check for outstanding acks
                  ZStream
                    .fromIterator(
                      acks.toList.commit.map(_.iterator)
                    )
                    .collectM {
                      case (ackId, ack0) =>
                        nodes.next
                          .zip(nodes.nodeState(ack0.target))
                          .flatMap {
                            case (Some(next), NodeState.Healthy) =>
                              nodes
                                .changeNodeState(ack0.target, NodeState.Unreachable)
                                .as(Message.Direct(next, PingReq(ack0.target, ackId)))
                            case (Some(_), NodeState.Unreachable) =>
                              ack(ackId) *>
                                nodes
                                  .changeNodeState(ack0.target, NodeState.Suspicion)
                                  .as(Message.Empty) //this should trigger suspicion mechanism
                            case (_, _) =>
                              nodes
                                .disconnect(ack0.target)
                                .as(Message.Empty)
                          }
                    }
                )
        )
      }
    } yield protocol

}
