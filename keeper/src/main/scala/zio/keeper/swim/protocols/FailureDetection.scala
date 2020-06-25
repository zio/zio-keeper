package zio.keeper.swim.protocols

import upickle.default._
import zio.duration._
import zio.keeper.swim.Nodes.{ NodeState, _ }
import zio.keeper.swim.{ ConversationId, LocalHealthMultiplier, Message, Nodes, Protocol }
import zio.keeper.{ ByteCodec, NodeAddress }
import zio.logging._
import zio.stm.{ STM, TMap }
import zio.stream.ZStream
import zio.{ Schedule, ZIO, keeper }

sealed trait FailureDetection

object FailureDetection {

  final case object Ping                                           extends FailureDetection
  final case object Ack                                            extends FailureDetection
  final case object Nack                                           extends FailureDetection
  final case class PingReq(target: NodeAddress)                    extends FailureDetection
  final case class Suspect(from: NodeAddress, nodeId: NodeAddress) extends FailureDetection
  final case class Alive(nodeId: NodeAddress)                      extends FailureDetection
  final case class Dead(nodeId: NodeAddress)                       extends FailureDetection

  implicit val ackCodec: ByteCodec[Ack.type] =
    ByteCodec.fromReadWriter(macroRW[Ack.type])

  implicit val nackCodec: ByteCodec[Nack.type] =
    ByteCodec.fromReadWriter(macroRW[Nack.type])

  implicit val pingCodec: ByteCodec[Ping.type] =
    ByteCodec.fromReadWriter(macroRW[Ping.type])

  implicit val pingReqCodec: ByteCodec[PingReq] =
    ByteCodec.fromReadWriter(macroRW[PingReq])

  implicit val suspectCodec: ByteCodec[Suspect] =
    ByteCodec.fromReadWriter(macroRW[Suspect])

  implicit val aliveCodec: ByteCodec[Alive] =
    ByteCodec.fromReadWriter(macroRW[Alive])

  implicit val deadCodec: ByteCodec[Dead] =
    ByteCodec.fromReadWriter(macroRW[Dead])

  implicit val byteCodec: ByteCodec[FailureDetection] =
    ByteCodec.tagged[FailureDetection][
      Ack.type,
      Ping.type,
      PingReq,
      Nack.type,
      Suspect,
      Alive,
      Dead
    ]

  def protocol(protocolPeriod: Duration, protocolTimeout: Duration, localNode: NodeAddress) =
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
                    } <* LocalHealthMultiplier.decrease
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
              case Message.Direct(sender, _, Suspect(_, `localNode`)) =>
                Message
                  .direct(sender, Alive(localNode))
                  .map(
                    Message.Batch(
                      _,
                      Message.Broadcast(Alive(localNode))
                    )
                  ) <* LocalHealthMultiplier.increase

              case Message.Direct(_, _, Suspect(_, node)) =>
                nodeState(node)
                  .orElseSucceed(NodeState.Dead)
                  .flatMap {
                    case NodeState.Dead | NodeState.Suspicion =>
                      Message.noResponse
                    case _ =>
                      changeNodeState(node, NodeState.Suspicion).ignore *>
                        Message.noResponse //it will trigger broadcast by events
                  }

              case Message.Direct(sender, _, msg @ Dead(nodeAddress)) if sender == nodeAddress =>
                changeNodeState(nodeAddress, NodeState.Left).ignore
                  .as(Message.Broadcast(msg))

              case Message.Direct(_, _, msg @ Dead(nodeAddress)) =>
                nodeState(nodeAddress).orElseSucceed(NodeState.Dead).flatMap {
                  case NodeState.Dead => Message.noResponse
                  case _ =>
                    changeNodeState(nodeAddress, NodeState.Dead).ignore
                      .as(Message.Broadcast(msg))
                }

              case Message.Direct(_, _, msg @ Alive(nodeAddress)) =>
                changeNodeState(nodeAddress, NodeState.Healthy).ignore
                  .as(Message.Broadcast(msg))
            },
            ZStream
              .repeatEffectWith(
                nextNode().zip(ConversationId.next),
                Schedule.forever.addDelayM(_ => LocalHealthMultiplier.scaleTimeout(protocolPeriod))
              )
              .collectM {
                case (Some((probedNode, state)), conversationId) =>
                  pendingAcks.put(conversationId, None).commit *>
                    LocalHealthMultiplier
                      .scaleTimeout(protocolTimeout)
                      .flatMap(
                        timeout =>
                          Message.withTimeout(
                            if (state != NodeState.Healthy)
                              Message.Batch(
                                Message.Direct(probedNode, conversationId, Ping),
                                //this is part of buddy system
                                Message.Direct(probedNode, conversationId, Suspect(localNode, probedNode))
                              )
                            else
                              Message.Direct(probedNode, conversationId, Ping),
                            pingTimeoutAction(
                              conversationId,
                              pendingAcks,
                              pendingNacks,
                              protocolTimeout,
                              probedNode,
                              localNode
                            ),
                            timeout
                          )
                      )

              }
          )
      }

  private def pingTimeoutAction(
    conversationId: Long,
    pendingAcks: TMap[Long, Option[(NodeAddress, Long)]],
    pendingNacks: TMap[Long, Unit],
    protocolTimeout: Duration,
    probedNode: NodeAddress,
    localNode: NodeAddress
  ): ZIO[LocalHealthMultiplier with Nodes with Logging, keeper.Error, Message[FailureDetection]] =
    pendingAcks
      .get(conversationId)
      .commit
      .flatMap {
        case Some(_) =>
          log.warn(s"node: $probedNode missed ack with id ${conversationId}") *>
            LocalHealthMultiplier.increase *>
            nextNode(Some(probedNode)).flatMap {
              case Some((next, _)) =>
                pendingNacks.put(conversationId, ()).commit *>
                  LocalHealthMultiplier
                    .scaleTimeout(protocolTimeout)
                    .flatMap(
                      timeout =>
                        Message.withTimeout(
                          Message.Direct(next, conversationId, PingReq(probedNode)),
                          pingReqTimeoutAction(
                            conversationId,
                            pendingAcks,
                            pendingNacks,
                            protocolTimeout,
                            probedNode,
                            localNode
                          ),
                          timeout
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
    conversationId: Long,
    pendingAcks: TMap[Long, Option[(NodeAddress, Long)]],
    pendingNacks: TMap[Long, Unit],
    protocolTimeout: Duration,
    probedNode: NodeAddress,
    localNode: NodeAddress
  ): ZIO[Nodes with LocalHealthMultiplier, keeper.Error, Message[FailureDetection]] =
    pendingAcks
      .get(conversationId)
      .commit
      .flatMap {
        case Some(_) =>
          pendingAcks.delete(conversationId).commit *>
            (LocalHealthMultiplier.increase *>
              pendingNacks
                .delete(conversationId)
                .commit)
              .whenM(pendingNacks.contains(conversationId).commit) *>
            changeNodeState(probedNode, NodeState.Suspicion) *>
            Message.withTimeout(
              Message.Broadcast(Suspect(localNode, probedNode)),
              ZIO.ifM(nodeState(probedNode).map(_ == NodeState.Suspicion).orElseSucceed(false))(
                changeNodeState(probedNode, NodeState.Dead)
                  .as(Message.Broadcast(Dead(probedNode))),
                Message.noResponse
              ),
              protocolTimeout
            )
        case None =>
          // PingReq acked already
          changeNodeState(probedNode, NodeState.Healthy) *>
            Message.noResponse
      }

}
