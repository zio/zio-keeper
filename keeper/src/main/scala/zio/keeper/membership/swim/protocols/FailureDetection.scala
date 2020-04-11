package zio.keeper.membership.swim.protocols

import upickle.default._
import zio.duration._
import zio.keeper.membership.swim.Nodes.NodeState
import zio.keeper.membership.swim.{Message, Nodes, Protocol}
import zio.keeper.membership.{ByteCodec, NodeAddress, TaggedCodec}
import zio.logging.Logging.Logging
import zio.logging._
import zio.stm.TMap
import zio.stream.ZStream
import zio.{Ref, Schedule, UIO, ZIO, keeper}

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

  private case class _Ack(onBehalf: Option[(NodeAddress, Long)])

  def protocol(nodes: Nodes, protocolPeriod: Duration, protocolTimeout: Duration) =
    for {
      acks  <- TMap.empty[Long, _Ack].commit
      ackId <- Ref.make(0L)
      protocol <- {

        def ack(id: Long) =
          (acks.get(id) <*
            acks
              .delete(id)).commit

        def withAck[R](
          onBehalf: Option[(NodeAddress, Long)],
          fn: Long => ZIO[R, zio.keeper.Error, Message.WithTimeout[FailureDetection]]
        ) =
          for {
            ackId      <- ackId.updateAndGet(_ + 1)
            nodeAndMsg <- fn(ackId)
            _          <- acks.put(ackId, _Ack(onBehalf)).commit
          } yield nodeAndMsg

        Protocol[FailureDetection](
          {
            case Message.Direct(sender, Ack(ackId)) =>
              log.debug(s"received ack[$ackId] from $sender") *>
                ack(ackId).flatMap {
                  case Some(_Ack(Some((node, originalAckId)))) =>
                    Message.direct(node, Ack(originalAckId))
                  case _ =>
                    Message.noResponse
                }
            case Message.Direct(sender, Ping(ackId)) =>
              ZIO.succeed(Message.Direct(sender, Ack(ackId)))

            case Message.Direct(sender, PingReq(to, originalAck)) =>
              withAck(
                Some((sender, originalAck)),
                ackId =>
                  Message.withTimeout(
                    Message.Direct(to, Ping(ackId)),
                    ack(ackId).as(Message.NoResponse),
                    protocolTimeout
                  )
              )

            case Message.Direct(_, Nack(_)) =>
              Message.noResponse
          },
          ZStream
            .repeatEffectWith(nodes.next, Schedule.spaced(protocolPeriod))
            .collectM {
              case Some(next) =>
                withAck(
                  None,
                  ackId =>
                    Message.withTimeout(
                      Message.Direct(next, Ping(ackId)),
                      onAckTimeout(nodes, next, ackId, acks, ack, protocolTimeout),
                      protocolTimeout
                    )
                )
            }
        )

      }
    } yield protocol

  private def onAckTimeout(
                            nodes: Nodes,
                            probedNode: NodeAddress,
                            ackId: Long,
                            acks: TMap[Long, _Ack],
                            deleteAck: Long => UIO[Option[_Ack]],
                            protocolTimeout: Duration
  ): ZIO[Logging, keeper.Error, Message[FailureDetection]] =
    acks.get(ackId).commit.flatMap {
      case Some(_) =>
        log.warn(s"node: $probedNode missed ack with id $ackId") *>
        nodes
          .changeNodeState(probedNode, NodeState.Unreachable) *>
        nodes.next.flatMap{
          case Some(next) =>
            Message.withTimeout(
              Message.Direct(next, PingReq(probedNode, ackId)),
              deleteAck(ackId).flatMap{
                case Some(_) =>
                  nodes
                    .changeNodeState(probedNode, NodeState.Suspicion) *>
                    Message.noResponse
                case None =>
                  Message.noResponse
              },
              protocolTimeout
            )
          case None =>
            nodes
              .changeNodeState(probedNode, NodeState.Suspicion) *>
                Message.noResponse
        }
      case None =>
        Message.noResponse

    }

}
