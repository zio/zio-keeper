package zio.keeper.membership.swim.protocols

import upickle.default.macroRW
import zio.ZIO
import zio.duration.Duration
import zio.keeper.membership.swim.Nodes.{NodeState, NodeStateChanged, _}
import zio.keeper.membership.swim.{Message, Protocol}
import zio.keeper.{ByteCodec, NodeAddress}

sealed trait Suspicion

object Suspicion {

  implicit val byteCodec: ByteCodec[Suspicion] =
    ByteCodec.tagged[Suspicion][
      Suspect,
      Alive,
      Dead
    ]

  final case class Suspect(from: NodeAddress, nodeId: NodeAddress) extends Suspicion

  object Suspect {

    implicit val codecSuspect: ByteCodec[Suspect] =
      ByteCodec.fromReadWriter(macroRW[Suspect])
  }

  final case class Alive(nodeId: NodeAddress) extends Suspicion

  object Alive {

    implicit val codecAlive: ByteCodec[Alive] =
      ByteCodec.fromReadWriter(macroRW[Alive])
  }

  final case class Dead(nodeId: NodeAddress) extends Suspicion

  object Dead {

    implicit val codecDead: ByteCodec[Dead] =
      ByteCodec.fromReadWriter(macroRW[Dead])
  }

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
