package zio.keeper.membership.swim.protocols

import upickle.default.macroRW
import zio.ZIO
import zio.duration.Duration
import zio.keeper.membership.swim.Nodes.{ NodeState, NodeStateChanged }
import zio.keeper.{ ByteCodec, NodeAddress }
import zio.keeper.membership.swim.{ Message, Nodes, Protocol }
import zio.keeper.{ ByteCodec, NodeAddress }
import zio.stm.TMap

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

  def protocol(nodes: Nodes, local: NodeAddress, timeout: Duration) =
    for {
      suspects <- TMap.empty[NodeAddress, Unit].commit
      protocol <- Protocol[Suspicion].make(
                   {
                     case Message.Direct(sender, Suspect(_, `local`)) =>
                       ZIO.succeed(
                         Message.Batch(
                           Message.Direct(sender, Alive(local)),
                           Message.Broadcast(Alive(local))
                         )
                       )

                     case Message.Direct(_, Suspect(_, node)) =>
                       suspects
                         .get(node)
                         .commit
                         .zip(nodes.nodeState(node).orElseSucceed(NodeState.Dead))
                         .flatMap {
                           case (Some(_), _) =>
                             Message.noResponse
                           case (_, NodeState.Dead | NodeState.Suspicion) =>
                             Message.noResponse
                           case (None, _) =>
                             nodes.changeNodeState(node, NodeState.Suspicion).ignore *>
                               Message.noResponse //it will trigger broadcast by events
                         }

                     case Message.Direct(sender, msg @ Dead(nodeAddress)) if sender == nodeAddress =>
                       nodes
                         .changeNodeState(nodeAddress, NodeState.Left)
                         .ignore
                         .as(Message.Broadcast(msg))

                     case Message.Direct(_, msg @ Dead(nodeAddress)) =>
                       nodes.nodeState(nodeAddress).orElseSucceed(NodeState.Dead).flatMap {
                         case NodeState.Dead => Message.noResponse
                         case _ =>
                           nodes
                             .changeNodeState(nodeAddress, NodeState.Dead)
                             .ignore
                             .as(Message.Broadcast(msg))
                       }

                     case Message.Direct(_, msg @ Alive(nodeAddress)) =>
                       suspects.delete(nodeAddress).commit *>
                         nodes
                           .changeNodeState(nodeAddress, NodeState.Healthy)
                           .ignore
                           .as(Message.Broadcast(msg))
                   },
                   nodes.internalEvents.collectM {
                     case NodeStateChanged(node, _, NodeState.Suspicion) =>
                       suspects
                         .put(
                           node,
                           ()
                         )
                         .commit
                         .flatMap(
                           _ =>
                             Message.withTimeout(
                               Message.Broadcast(Suspect(local, node)),
                               ZIO.ifM(suspects.contains(node).commit)(
                                 nodes
                                   .changeNodeState(node, NodeState.Dead)
                                   .as(Message.Broadcast(Dead(node))),
                                 Message.noResponse
                               ),
                               timeout
                             )
                         )
                   }
                 )
    } yield protocol

}
