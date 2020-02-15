package zio.keeper.membership.swim.protocols

import upickle.default._
import zio.ZIO
import zio.keeper.discovery.Discovery
import zio.keeper.membership.swim.{ Nodes, Protocol }
import zio.keeper.{ ByteCodec, TaggedCodec }
import zio.logging.Logging
import zio.logging.slf4j._
import zio.stream.ZStream

sealed trait Initial

object Initial {

  implicit def taggedRequests(
    implicit
    c4: ByteCodec[Join.type],
    c6: ByteCodec[Accept.type],
    c7: ByteCodec[Reject]
  ): TaggedCodec[Initial] =
    TaggedCodec.instance(
      {
        case Join      => 13
        case Accept    => 15
        case _: Reject => 16
      }, {
        case 13 => c4.asInstanceOf[ByteCodec[Initial]]
        case 15 => c6.asInstanceOf[ByteCodec[Initial]]
        case 16 => c7.asInstanceOf[ByteCodec[Initial]]
      }
    )

  final case object Join extends Initial

  implicit def codecJoin[A: ReadWriter]: ByteCodec[Join.type] =
    ByteCodec.fromReadWriter(macroRW[Join.type])

  case object Accept extends Initial

  implicit def codecAccept[A: ReadWriter]: ByteCodec[Accept.type] =
    ByteCodec.fromReadWriter(macroRW[Accept.type])

  case class Reject(msg: String) extends Initial

  object Reject {

    implicit def codec[A: ReadWriter]: ByteCodec[Reject] =
      ByteCodec.fromReadWriter(macroRW[Reject])
  }

  def protocol[A: ReadWriter](nodes: Nodes[A])(implicit codec: TaggedCodec[Initial]) =
    ZIO.access[Discovery[A] with Logging[String]](
      env =>
        Protocol[A, Initial](
          {
            case (sender, Join) =>
              nodes.established(sender).as(Some((sender, Accept)))
            case (sender, Accept) =>
              nodes.established(sender).as(None)
            case (sender, Reject(msg)) =>
              logger.error("Rejected from cluster: " + msg)
              nodes.disconnect(sender).as(None)
          },
          ZStream
            .fromIterator(
              env.discover.discoverNodes.map(_.iterator)
            )
            .mapM(node => nodes.connect(node).as((node, Join)))
        )
    )

}
