package zio.keeper.membership.swim.protocols

import upickle.default._
import zio.duration._
import zio.keeper.membership.swim.Nodes.NodeState
import zio.keeper.membership.swim.{ Message, NodeId, Nodes, Protocol }
import zio.keeper.membership.{ ByteCodec, NodeAddress, TaggedCodec }
import zio.stm.TMap
import zio.stream.ZStream
import zio.{ Ref, Schedule, ZIO }
import zio.logging._

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
        case _: Nack    => 13
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

  final case class PingReq(target: NodeAddress, ackConversation: Long) extends FailureDetection

  object PingReq {

    implicit val codec: ByteCodec[PingReq] =
      ByteCodec.fromReadWriter(macroRW[PingReq])
  }

  private case class _Ack(target: NodeAddress, onBehalf: Option[(NodeAddress, Long)])

  def protocol(nodes: Nodes, protocolPeriod: Duration) =
    for {
      acks  <- TMap.empty[Long, _Ack].commit
      ackId <- Ref.make(0L)
      protocol <- {

        def ack(id: Long) =
          (acks.get(id) <*
            acks
              .delete(id)).commit

        def withAck(onBehalf: Option[(NodeAddress, Long)], fn: Long => Message.Direct[FailureDetection]) =
          for {
            ackId      <- ackId.updateAndGet(_ + 1)
            nodeAndMsg = fn(ackId)
            _          <- acks.put(ackId, _Ack(nodeAndMsg.node, onBehalf)).commit
          } yield nodeAndMsg

        Protocol[FailureDetection](
          {
            case Message.Direct(_, Ack(ackId)) =>
              ack(ackId).map {
                case Some(_Ack(_, Some((node, originalAckId)))) =>
                  Some(Message.Direct(node, Ack(originalAckId)))
                case _ =>
                  None
              }
            case Message.Direct(sender, Ping(ackId)) =>
              ZIO.succeed(Some(Message.Direct(sender, Ack(ackId))))

            case Message.Direct(sender, PingReq(to, originalAck)) =>
              withAck(Some((sender, originalAck)), ackId => Message.Direct(to, Ping(ackId))).map(Some(_))

            case Message.Direct(_, Nack(_)) =>
              ZIO.succeed(None)
          },
          ZStream
            .repeatEffectWith(
              log.info("start failure detection round.") *> nodes.next,
              Schedule.spaced(protocolPeriod)
            ).collectM {
              case Some(next) =>
                withAck(None, ackId => Message.Direct(next, Ping(ackId)))
            }
//            *>
//              ZStream
//                .fromEffect(nodes.next)
//                .collectM {
//                  case Some(next) =>
//                    withAck(None, ackId => Message.Direct(next, Ping(ackId)))
//                }
//                .merge(
//                  // Every protocol round check for outstanding acks
//                  ZStream
//                    .fromIterator(
//                      acks.toList.commit.map(_.iterator)
//                    )
//                    .collectM {
//                      case (ackId, ack0) =>
//                        nodes
//                          .nodeState(ack0.target)
//                          .flatMap {
//                            case NodeState.Healthy =>
//                              nodes.next.flatMap {
//                                case Some(next) =>
//                                  nodes
//                                    .changeNodeState(ack0.target, NodeState.Unreachable)
//                                    .as(Some(Message.Direct(next, PingReq(ack0.target, ackId))))
//                                case None =>
//                                  nodes
//                                    .disconnect(ack0.target)
//                                    .as(None)
//                              }
//                            case NodeState.Unreachable =>
//                              ack(ackId) *>
//                                nodes
//                                  .changeNodeState(ack0.target, NodeState.Suspicion)
//                                  .as(None) //this should trigger suspicion mechanism
//                            case _ => ZIO.succeed(None)
//                          }
//                    }
//                    .collect {
//                      case Some(r) => r
//                    }
//                )
        )
      }
    } yield protocol

}
