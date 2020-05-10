package zio.keeper.membership.swim.protocols

import upickle.default.macroRW
import zio.ZIO
import zio.duration.Duration
import zio.keeper.membership.swim.Nodes._
import zio.keeper.membership.swim.{ Message, Protocol }
import zio.keeper.{ ByteCodec, NodeAddress, TaggedCodec }

sealed trait Suspicion

object Suspicion {

  implicit def taggedRequests(
    implicit
    c4: ByteCodec[Suspect],
    c6: ByteCodec[Alive],
    c7: ByteCodec[Dead]
  ): TaggedCodec[Suspicion] =
    TaggedCodec.instance(
      {
        case _: Suspect => 33
        case _: Alive   => 35
        case _: Dead    => 36
      }, {
        case 33 => c4.asInstanceOf[ByteCodec[Suspicion]]
        case 35 => c6.asInstanceOf[ByteCodec[Suspicion]]
        case 36 => c7.asInstanceOf[ByteCodec[Suspicion]]
      }
    )

  final case class Suspect(from: NodeAddress, nodeId: NodeAddress) extends Suspicion

  implicit val codecSuspect: ByteCodec[Suspect] =
    ByteCodec.fromReadWriter(macroRW[Suspect])

  final case class Alive(nodeId: NodeAddress) extends Suspicion

  implicit val codecAlive: ByteCodec[Alive] =
    ByteCodec.fromReadWriter(macroRW[Alive])

  final case class Dead(nodeId: NodeAddress) extends Suspicion

  implicit val codecDead: ByteCodec[Dead] =
    ByteCodec.fromReadWriter(macroRW[Dead])

  def protocol(local: NodeAddress, timeout: Duration) =
    Protocol[Suspicion].make(
      {
        case Message.Direct(sender, _, Suspect(_, `local`)) =>
          Message
            .direct(sender, Alive(local))
            .map(
              Message.Batch(
                _,
                Message.Broadcast(Alive(local))
              )
            )

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
      internalEvents.collectM {
        case NodeStateChanged(node, _, NodeState.Suspicion) =>
          Message.withTimeout(
            Message.Broadcast(Suspect(local, node)),
            ZIO.ifM(nodeState(node).map(_ == NodeState.Suspicion).orElseSucceed(false))(
              changeNodeState(node, NodeState.Dead)
                .as(Message.Broadcast(Dead(node))),
              Message.noResponse
            ),
            timeout
          )
      }
    )

}
