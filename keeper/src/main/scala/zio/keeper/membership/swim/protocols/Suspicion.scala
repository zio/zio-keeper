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

import scala.concurrent.duration.Deadline

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

  private case class _Suspicion(start: Long, confirmations: Set[NodeAddress], requiredConfirmations: Int, deadline: Deadline)

  def protocol(nodes: Nodes, local: NodeAddress, suspicionMultiplier: Int, interval: Duration) =
    for {
      suspects <- TMap.empty[NodeAddress, _Suspicion].commit
      protocol <- Protocol[Suspicion](
        {
          case Message.Direct(sender, Suspect(`local`)) =>
            ZIO.succeed(
              Message.Batch(
                Message.Direct(sender, Alive(local)),
                Message.Broadcast(Alive(local))
              ))

          case Message.Direct(_, Suspect(node)) =>
            nodes.changeNodeState(node, NodeState.Suspicion).ignore
              .as(Message.NoResponse) //it will trigger broadcast by events

          case Message.Direct(sender, msg@Dead(nodeAddress)) if sender == nodeAddress =>
            nodes.changeNodeState(nodeAddress, NodeState.Left).ignore
              .as(Message.Broadcast(msg))

          case Message.Direct(_, msg@Dead(nodeAddress)) =>
            nodes.changeNodeState(nodeAddress, NodeState.Death).ignore
              .as(Message.Broadcast(msg))

          case Message.Direct(_, msg@Alive(nodeAddress)) =>
            nodes.changeNodeState(nodeAddress, NodeState.Healthy).ignore
              .as(Message.Broadcast(msg))
        },
        nodes.events.collectM {
          case MembershipEvent.NodeStateChanged(node, _, NodeState.Suspicion) =>
            for {
              time <- currentTime(TimeUnit.MILLISECONDS)
              numOfNodes <- nodes.numberOfNodes
              requiredConfirmations = if(numOfNodes < suspicionMultiplier) 0 else suspicionMultiplier - 2
              _ <- suspects.put(
                node,
                _Suspicion(
                  time,
                  Set.empty,
                  requiredConfirmations,
                  suspicionTimeout(suspicionMultiplier, numOfNodes, interval)
                )
              ).commit
            } yield Message.Broadcast(Suspect(node))
        }
      )
    } yield protocol


  private def suspicionTimeout(suspicionMultiplier: Int, numberOfNodes: Int, protocolInterval: Duration) = {
    val nodeScale = math.max(1.0, math.log10(math.max(1.0, numberOfNodes.toDouble)))
    val timeout = protocolInterval * (suspicionMultiplier * nodeScale)
    timeout.fold(Deadline.now, finite => Deadline.now + finite.asScala)
  }
}
