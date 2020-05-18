package zio.keeper.membership.swim.protocols

import upickle.default._
import zio.duration._
import zio.keeper.{ ByteCodec, NodeAddress }
import zio.keeper.membership.swim.Nodes.NodeState
import zio.keeper.membership.swim.{ Message, Nodes, Protocol }
import zio.logging.Logging
import zio.keeper.membership.swim.Nodes._
import zio.keeper.membership.swim.{ Message, Protocol }
import zio.keeper.{ ByteCodec, NodeAddress, TaggedCodec }
import zio.logging._
import zio.stm.TMap
import zio.stream.ZStream
import zio.{ Schedule, ZIO }

sealed trait FailureDetection

object FailureDetection {

  implicit val byteCodec: ByteCodec[FailureDetection] =
    ByteCodec.tagged[FailureDetection][
      Ack,
      Ping,
      PingReq,
      Nack
    ]

  final case object Ack extends FailureDetection

  implicit val ackCodec: ByteCodec[Ack.type] =
    ByteCodec.fromReadWriter(macroRW[Ack.type])

  final case object Nack extends FailureDetection

  implicit val nackCodec: ByteCodec[Nack.type] =
    ByteCodec.fromReadWriter(macroRW[Nack.type])

  final case object Ping extends FailureDetection

  implicit val pingCodec: ByteCodec[Ping.type] =
    ByteCodec.fromReadWriter(macroRW[Ping.type])

  final case class PingReq(target: NodeAddress) extends FailureDetection

  object PingReq {

    implicit val codec: ByteCodec[PingReq] =
      ByteCodec.fromReadWriter(macroRW[PingReq])
  }

  def protocol(protocolPeriod: Duration, protocolTimeout: Duration) =
    for {
      pendingAcks <- TMap.empty[Long, Option[(NodeAddress, Long)]].commit
      protocol <- {
        Protocol[FailureDetection].make(
          {
            case Message.Direct(sender, conversationId, Ack) =>
              log.debug(s"received ack[$conversationId] from $sender") *>
                pendingAcks.get(conversationId).tap(_ => pendingAcks.delete(conversationId)).commit.flatMap {
                  case Some(Some((node, originalAckId))) =>
                    ZIO.succeedNow(Message.Direct(node, originalAckId, Ack))
                  case _ =>
                    Message.noResponse
                }
            case Message.Direct(sender, conversationId, Ping) =>
              ZIO.succeedNow(Message.Direct(sender, conversationId, Ack))

            case Message.Direct(sender, originalAck, PingReq(to)) =>
              for {
                ping <- Message.direct(to, Ping)
                _    <- pendingAcks.put(ping.conversationId, Some((sender, originalAck))).commit
                withTimeout <- Message.withTimeout(
                                message = ping,
                                action = pendingAcks.delete(ping.conversationId).commit.as(Message.NoResponse),
                                timeout = protocolTimeout
                              )
              } yield withTimeout
            case Message.Direct(_, _, Nack) =>
              Message.noResponse
          },
          ZStream
            .repeatEffectWith(nextNode, Schedule.spaced(protocolPeriod))
            .collectM {
              case Some(probedNode) =>
                Message
                  .direct(probedNode, Ping)
                  .tap(msg => pendingAcks.put(msg.conversationId, None).commit)
                  .flatMap(
                    msg =>
                      Message.withTimeout(
                        msg,
                        pendingAcks.get(msg.conversationId).commit.flatMap {
                          case Some(_) =>
                            log.warn(s"node: $probedNode missed ack with id ${msg.conversationId}") *>
                              changeNodeState(probedNode, NodeState.Unreachable) *>
                              nextNode.flatMap {
                                case Some(next) =>
                                  Message.withTimeout(
                                    Message.Direct(next, msg.conversationId, PingReq(probedNode)),
                                    pendingAcks.get(msg.conversationId).commit.flatMap {
                                      case Some(_) =>
                                        pendingAcks.delete(msg.conversationId).commit *>
                                          changeNodeState(probedNode, NodeState.Suspicion) *>
                                          Message.noResponse
                                      case None =>
                                        Message.noResponse
                                    },
                                    protocolTimeout
                                  )
                                case None =>
                                  changeNodeState(probedNode, NodeState.Dead) *>
                                    Message.noResponse
                              }
                          case None =>
                            Message.noResponse
                        },
                        protocolTimeout
                      )
                  )
            }
        )
      }
    } yield protocol

}
