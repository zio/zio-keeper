package zio.keeper.membership.swim.protocols

import upickle.default.macroRW
import zio.ZIO
import zio.duration.Duration
import zio.keeper.membership.swim.Nodes.{ NodeState, NodeStateChanged }
import zio.keeper.membership.swim.{ Message, Nodes, Protocol }
import zio.keeper.{ ByteCodec, NodeAddress, TaggedCodec }
import zio.stm.TMap

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
                         .zip(nodes.nodeState(node).orElseSucceed(NodeState.Death))
                         .flatMap {
                           case (Some(_), _) =>
                             Message.noResponse
                           case (_, NodeState.Death | NodeState.Suspicion) =>
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
                       nodes.nodeState(nodeAddress).orElseSucceed(NodeState.Death).flatMap {
                         case NodeState.Death => Message.noResponse
                         case _ =>
                           nodes
                             .changeNodeState(nodeAddress, NodeState.Death)
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
                                   .changeNodeState(node, NodeState.Death)
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
