package zio.keeper.swim.protocols

import upickle.default._
import zio.clock.Clock
import zio.duration._
import zio.keeper.{ ByteCodec, NodeAddress }
import zio.keeper.swim.Nodes.{ NodeState, _ }
import zio.keeper.swim.{ ConversationId, Message, Nodes, Protocol }
import zio.logging._
import zio.stm.TMap
import zio.stream.ZStream
import zio.{ Schedule, ZIO, keeper }

sealed trait FailureDetection

object FailureDetection {

  final case object Ping                        extends FailureDetection
  final case object Ack                         extends FailureDetection
  final case object Nack                        extends FailureDetection
  final case class PingReq(target: NodeAddress) extends FailureDetection

  implicit val ackCodec: ByteCodec[Ack.type] =
    ByteCodec.fromReadWriter(macroRW[Ack.type])

  implicit val nackCodec: ByteCodec[Nack.type] =
    ByteCodec.fromReadWriter(macroRW[Nack.type])

  implicit val pingCodec: ByteCodec[Ping.type] =
    ByteCodec.fromReadWriter(macroRW[Ping.type])

  implicit val pingReqCodec: ByteCodec[PingReq] =
    ByteCodec.fromReadWriter(macroRW[PingReq])

  implicit val byteCodec: ByteCodec[FailureDetection] =
    ByteCodec.tagged[FailureDetection][
      Ack.type,
      Ping.type,
      PingReq,
      Nack.type
    ]

  type Env = Logging with ConversationId with Nodes with Clock

  def protocol(
    protocolPeriod: Duration,
    protocolTimeout: Duration
  ): ZIO[Env, keeper.Error, Protocol[FailureDetection]] =
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
                                        changeNodeState(probedNode, NodeState.Healthy) *>
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
