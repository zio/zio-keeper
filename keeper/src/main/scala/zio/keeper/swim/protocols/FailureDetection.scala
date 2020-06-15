package zio.keeper.swim.protocols

import upickle.default._
import zio.duration._
import zio.keeper.swim.Nodes.{ NodeState, _ }
import zio.keeper.swim.{ LocalHealthAwareness, Message, Nodes, Protocol }
import zio.keeper.{ ByteCodec, NodeAddress }
import zio.logging._
import zio.stm.{ STM, TMap }
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

  def protocol(protocolPeriod: Duration, protocolTimeout: Duration) =
    TMap
      .empty[Long, Option[(NodeAddress, Long)]]
      .zip(TMap.empty[Long, Unit])
      .commit
      .flatMap {
        case (pendingAcks, pendingNacks) =>
          Protocol[FailureDetection].make(
            {
              case Message.Direct(sender, conversationId, Ack) =>
                log.debug(s"received ack[$conversationId] from $sender") *>
                  pendingAcks
                    .get(conversationId)
                    .tap(_ => pendingAcks.delete(conversationId))
                    .commit
                    .flatMap {
                      case Some(Some((node, originalAckId))) =>
                        ZIO.succeedNow(Message.Direct(node, originalAckId, Ack))
                      case _ =>
                        Message.noResponse
                    } <* LocalHealthAwareness.decrease
              case Message.Direct(sender, conversationId, Ping) =>
                ZIO.succeedNow(Message.Direct(sender, conversationId, Ack))

              case Message.Direct(sender, originalAck, PingReq(to)) =>
                for {
                  ping <- Message.direct(to, Ping)
                  _    <- pendingAcks.put(ping.conversationId, Some((sender, originalAck))).commit
                  withTimeout <- Message.withTimeout(
                                  message = ping,
                                  action = pendingAcks
                                    .get(ping.conversationId)
                                    .flatMap {
                                      case Some(Some((sender, originalAck))) =>
                                        pendingAcks
                                          .delete(ping.conversationId)
                                          .as(Message.Direct(sender, originalAck, Nack))
                                      case _ =>
                                        STM.succeedNow(Message.NoResponse)
                                    }
                                    .commit,
                                  timeout = protocolTimeout
                                )
                } yield withTimeout
              case Message.Direct(_, conversationId, Nack) =>
                pendingNacks.delete(conversationId).commit *>
                  Message.noResponse
            },
            ZStream
              .repeatEffectWith(
                nextNode,
                Schedule.forever.addDelayM(_ => LocalHealthAwareness.scaleTimeout(protocolPeriod))
              )
              .collectM {
                case Some(probedNode) =>
                  Message
                    .direct(probedNode, Ping)
                    .tap(msg => pendingAcks.put(msg.conversationId, None).commit)
                    .zip(LocalHealthAwareness.scaleTimeout(protocolTimeout))
                    .flatMap {
                      case (msg, timeout) =>
                        Message.withTimeout(
                          message = msg,
                          action = pingTimeoutAction(msg, pendingAcks, pendingNacks, protocolTimeout, probedNode),
                          timeout = timeout
                        )
                    }
              }
          )
      }

  private def pingTimeoutAction(
    msg: Message.Direct[FailureDetection.Ping.type],
    pendingAcks: TMap[Long, Option[(NodeAddress, Long)]],
    pendingNacks: TMap[Long, Unit],
    protocolTimeout: Duration,
    probedNode: NodeAddress
  ): ZIO[LocalHealthAwareness with Nodes with Logging, keeper.Error, Message[PingReq]] =
    pendingAcks
      .get(msg.conversationId)
      .commit
      .flatMap {
        case Some(_) =>
          log.warn(s"node: $probedNode missed ack with id ${msg.conversationId}") *>
            changeNodeState(probedNode, NodeState.Unreachable) *>
            LocalHealthAwareness.increase *>
            nextNode.flatMap {
              case Some(next) =>
                pendingNacks.put(msg.conversationId, ()).commit *>
                  LocalHealthAwareness
                    .scaleTimeout(protocolTimeout)
                    .flatMap(
                      timeout =>
                        Message.withTimeout(
                          message = Message.Direct(next, msg.conversationId, PingReq(probedNode)),
                          action = pingReqTimeoutAction(msg, pendingAcks, pendingNacks, probedNode),
                          timeout = timeout
                        )
                    )

              case None =>
                // we don't know any other node to ask
                changeNodeState(probedNode, NodeState.Dead) *>
                  Message.noResponse
            }
        case None =>
          // Ping was ack already
          Message.noResponse
      }

  private def pingReqTimeoutAction(
    msg: Message.Direct[FailureDetection.Ping.type],
    pendingAcks: TMap[Long, Option[(NodeAddress, Long)]],
    pendingNacks: TMap[Long, Unit],
    probedNode: NodeAddress
  ): ZIO[Nodes, keeper.Error, Message.NoResponse.type] =
    pendingAcks
      .get(msg.conversationId)
      .commit
      .flatMap {
        case Some(_) =>
          pendingAcks.delete(msg.conversationId).commit *>
            (LocalHealthAwareness.increase *>
              pendingNacks
                .delete(msg.conversationId)
                .commit)
              .whenM(pendingNacks.contains(msg.conversationId).commit)
          changeNodeState(probedNode, NodeState.Suspicion) *>
            Message.noResponse
        case None =>
          // PingReq acked already
          changeNodeState(probedNode, NodeState.Healthy) *>
            Message.noResponse
      }

}
