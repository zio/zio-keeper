package zio.keeper.membership.swim.protocols

import java.util.concurrent.TimeUnit

import upickle.default.macroRW
import zio.ZIO
import zio.clock._
import zio.duration.Duration
import zio.keeper.membership.swim.Nodes.NodeState
import zio.keeper.membership.swim.{Message, Nodes, Protocol}
import zio.keeper.membership.{ByteCodec, MembershipEvent, NodeAddress, TaggedCodec}
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

  case class Suspect(from: NodeAddress, nodeId: NodeAddress) extends Suspicion

  implicit val codecSuspect: ByteCodec[Suspect] =
    ByteCodec.fromReadWriter(macroRW[Suspect])

  case class Alive(nodeId: NodeAddress) extends Suspicion

  implicit val codecAlive: ByteCodec[Alive] =
    ByteCodec.fromReadWriter(macroRW[Alive])

  case class Dead(nodeId: NodeAddress) extends Suspicion

  implicit val codecDead: ByteCodec[Dead] =
    ByteCodec.fromReadWriter(macroRW[Dead])


  def protocol(nodes: Nodes, local: NodeAddress, timeout: Duration) =
    for {
      suspects <- TMap.empty[NodeAddress, Unit].commit
      protocol <- Protocol[Suspicion](
        {
          case Message.Direct(sender, Suspect(_, `local`)) =>
            ZIO.succeed(
              Message.Batch(
                Message.Direct(sender, Alive(local)),
                Message.Broadcast(Alive(local))
              ))

          case Message.Direct(_, Suspect(from, node)) =>
            suspects.get(node).commit.flatMap{
              case Some(_) =>
                Message.noResponse
              case None =>
                nodes.changeNodeState(node, NodeState.Suspicion).ignore *>
                  Message.noResponse//it will trigger broadcast by events
            }

          case Message.Direct(sender, msg@Dead(nodeAddress)) if sender == nodeAddress =>
            nodes.changeNodeState(nodeAddress, NodeState.Left).ignore
              .as(Message.Broadcast(msg))

          case Message.Direct(_, msg@Dead(nodeAddress)) =>
            nodes.changeNodeState(nodeAddress, NodeState.Death).ignore
              .as(Message.Broadcast(msg))

          case Message.Direct(_, msg@Alive(nodeAddress)) =>
            suspects.delete(nodeAddress).commit *>
            nodes.changeNodeState(nodeAddress, NodeState.Healthy).ignore
              .as(Message.Broadcast(msg))
        },
        nodes.events.collectM {
          case MembershipEvent.NodeStateChanged(node, _, NodeState.Suspicion) =>
            suspects.put(
              node,
              ()
            ).commit.flatMap( _ =>
              Message.withTimeout(
                Message.Broadcast(Suspect(local, node)),
                ZIO.ifM(suspects.contains(node).commit) (
                  nodes.changeNodeState(node, NodeState.Death)
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
