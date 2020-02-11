package zio.keeper.membership.swim.protocols

import upickle.default._
import zio.keeper.membership.NodeId
import zio.keeper.membership.swim.GossipState
import zio.keeper.{ByteCodec, TaggedCodec}

sealed trait Initial[A]

object Initial {

  implicit def taggedRequests[A](
                                  implicit
                                  c4: ByteCodec[Join[A]],
                                  c5: ByteCodec[JoinCluster[A]],
                                  c6: ByteCodec[Accept[A]],
                                  c7: ByteCodec[Reject[A]]

                                ): TaggedCodec[Initial[A]] =
    TaggedCodec.instance(
      {
        case _: Join[A]    => 13
        case _: JoinCluster[A]    => 14
        case _: Accept[A] => 15
        case _: Reject[A] => 16
      }, {
        case 13 => c4.asInstanceOf[ByteCodec[Initial[A]]]
        case 14 => c5.asInstanceOf[ByteCodec[Initial[A]]]
        case 15 => c6.asInstanceOf[ByteCodec[Initial[A]]]
        case 16 => c7.asInstanceOf[ByteCodec[Initial[A]]]
      }
    )

  final case class Join[A](nodeId: NodeId, state: GossipState[A], address: A) extends Initial[A]

  object Join {
    implicit def codec[A: ReadWriter]: ByteCodec[Join[A]] =
      ByteCodec.fromReadWriter(macroRW[Join[A]])
  }

  case class Accept[A](state: GossipState[A], address: A) extends Initial[A]

  object Accept {
    implicit def codec[A: ReadWriter]: ByteCodec[Accept[A]] =
      ByteCodec.fromReadWriter(macroRW[Accept[A]])
  }

  case class Reject[A](msg: String) extends Initial[A]

  object Reject {
    implicit def codec[A: ReadWriter]: ByteCodec[Reject[A]] =
      ByteCodec.fromReadWriter(macroRW[Reject[A]])
  }

  final case class JoinCluster[A](state: GossipState[A], address: A) extends Initial[A]

  object JoinCluster {
    implicit def codec[A: ReadWriter]: ByteCodec[JoinCluster[A]] =
      ByteCodec.fromReadWriter(macroRW[JoinCluster[A]])
  }

//  def protocol[A](localMember: Member[A])(implicit tg: TaggedCodec[Initial[A]]) =
//    Protocol[Gossip[A], Initial[A]] {
//      case Initial.Join(nodeId, _, _) => for {
//        gossip <- ZIO.access[Gossip[A]](_.gossip)
//        nodeState <- gossip.nodeState(nodeId)
//        reply <- nodeState match {
//          case NodeState.New =>
//            gossip.modifyNodeState(nodeId, _ => NodeState.Healthy) *>
//              gossip.state.flatMap(state => Protocol.reply[Initial[A]](Accept[A](state, localMember.addr)))
//          case NodeState.Healthy =>
//            Protocol.reply[Initial[A]](Reject[A]("duplicate Node Id"))
//        }
//      } yield reply
//    }

}

