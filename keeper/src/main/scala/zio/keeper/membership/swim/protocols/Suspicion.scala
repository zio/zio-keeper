package zio.keeper.membership.swim.protocols

import upickle.default.macroRW
import zio.ZIO
import zio.keeper.membership.swim.Nodes.NodeState
import zio.keeper.membership.swim.{Message, Nodes, Protocol}
import zio.keeper.membership.{ByteCodec, MembershipEvent, NodeAddress, TaggedCodec}

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

  case class Suspect(nodeId: NodeAddress) extends Suspicion

  implicit val codecSuspect: ByteCodec[Suspect] =
    ByteCodec.fromReadWriter(macroRW[Suspect])

  case class Alive(nodeId: NodeAddress) extends Suspicion

  implicit val codecAlive: ByteCodec[Alive] =
    ByteCodec.fromReadWriter(macroRW[Alive])

  case class Dead(nodeId: NodeAddress) extends Suspicion

  implicit val codecDead: ByteCodec[Dead] =
    ByteCodec.fromReadWriter(macroRW[Dead])

  def protocol(nodes: Nodes, local: NodeAddress) = Protocol[Suspicion](
    {
      case Message.Direct(sender, Suspect(`local`)) =>
        ZIO.succeed(Message.Direct(sender, Alive(local)))
      case Message.Direct(_, Suspect(node)) =>
        nodes.changeNodeState(node, NodeState.Suspicion).ignore
          .as(Message.NoResponse) //it will trigger broadcast by events
      case _ => ZIO.succeed(Message.NoResponse)
    },
    nodes.events.collect{
      case MembershipEvent.NodeStateChanged(node, _, NodeState.Suspicion) =>
        Message.Broadcast(Suspect(node))
    }
  )
}
