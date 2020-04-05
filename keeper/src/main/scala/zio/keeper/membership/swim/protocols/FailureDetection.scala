package zio.keeper.membership.swim.protocols

import upickle.default._
import zio.duration._
import zio.keeper.membership.swim.Nodes.NodeState
import zio.keeper.membership.swim.{Message, Nodes, Protocol}
import zio.keeper.membership.{ByteCodec, NodeAddress, TaggedCodec}
import zio.logging._
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
            case Message.Direct(sender, Ack(ackId)) =>
              ack(ackId).flatMap {
                case Some(_Ack(_, Some((node, originalAckId)))) =>
                  nodes.changeNodeState(sender, NodeState.Healthy).as(
                    Message.Direct(node, Ack(originalAckId))
                  )
                case _ =>
                  ZIO.succeed(Message.NoResponse)
              }
            case Message.Direct(sender, Ping(ackId)) =>
              ZIO.succeed(Message.Direct(sender, Ack(ackId)))

            case Message.Direct(sender, PingReq(to, originalAck)) =>
              withAck(Some((sender, originalAck)), ackId => Message.Direct(to, Ping(ackId)))

            case Message.Direct(_, Nack(_)) =>
              ZIO.succeed(Message.NoResponse)
          },
          ZStream
            .repeatEffectWith(
              log.info("start failure detection round."),
              Schedule.spaced(protocolPeriod)
            )
            *>
              ZStream
                .fromEffect(nodes.next)
                .collectM {
                  case Some(next) =>
                    withAck(None, ackId => Message.Direct(next, Ping(ackId)))
                }
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
//                              nodes
//                                .changeNodeState(ack0.target, NodeState.Unreachable) *>
//                              nodes.next.flatMap {
//                                case Some(next) =>
//                                    ZIO.succeed(Some(Message.Direct(next, PingReq(ack0.target, ackId))))
//                                case None =>
//                                  nodes
//                                    .changeNodeState(ack0.target, NodeState.Suspicion)
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
