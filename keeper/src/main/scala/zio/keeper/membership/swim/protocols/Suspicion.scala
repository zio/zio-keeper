package zio.keeper.membership.swim.protocols

import upickle.default.macroRW
import zio.ZIO
import zio.keeper.membership.swim.{NodeId, Nodes, Protocol}
import zio.keeper.{ByteCodec, TaggedCodec}
import zio.stream.ZStream

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
        case _: Suspect   => 33
        case _: Alive    => 35
        case _: Dead => 36
      }, {
        case 33 => c4.asInstanceOf[ByteCodec[Suspicion]]
        case 35 => c6.asInstanceOf[ByteCodec[Suspicion]]
        case 36 => c7.asInstanceOf[ByteCodec[Suspicion]]
      }
    )

  case class Suspect(nodeId: NodeId) extends Suspicion
  implicit val codecSuspect: ByteCodec[Suspect] =
    ByteCodec.fromReadWriter(macroRW[Suspect])

  case class Alive(nodeId: NodeId) extends Suspicion

  implicit val codecAlive: ByteCodec[Alive] =
    ByteCodec.fromReadWriter(macroRW[Alive])

  case class Dead(nodeId: NodeId) extends Suspicion

  implicit val codecDead: ByteCodec[Dead] =
    ByteCodec.fromReadWriter(macroRW[Dead])

  def protocol(nodes: Nodes) = Protocol[Suspicion](
    {
      case _ => ZIO.succeed(None)
    },
    ZStream.empty
  )
}