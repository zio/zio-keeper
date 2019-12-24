package zio.membership.hyparview

import zio.membership.ByteCodec
import upickle.default._
import zio._
import zio.membership.Error
import zio.stream.ZStream

sealed private[hyparview] trait NeighborProtocol

private[hyparview] object NeighborProtocol {

  implicit val tagged: Tagged[NeighborProtocol] =
    Tagged.instance(
      {
        case _: Reject.type => 20
        case _: Accept.type => 21
      }, {
        case 20 => ByteCodec[Reject.type].asInstanceOf[ByteCodec[NeighborProtocol]]
        case 21 => ByteCodec[Accept.type].asInstanceOf[ByteCodec[NeighborProtocol]]
      }
    )

  case object Reject extends NeighborProtocol {

    implicit val codec: ByteCodec[Reject.type] =
      ByteCodec.fromReadWriter(macroRW[Reject.type])
  }

  case object Accept extends NeighborProtocol {

    implicit val codec: ByteCodec[Accept.type] =
      ByteCodec.fromReadWriter(macroRW[Accept.type])
  }

  def receive[R, R1 <: R, E >: Error, E1 >: E, A](
    stream: ZStream[R, E, Chunk[Byte]]
  )(
    contStream: ZStream[R, E, Chunk[Byte]] => ZStream[R1, E1, A]
  ): ZStream[R1, E1, A] =
    ZStream.unwrapManaged {
      stream.process.mapM { pull =>
        val continue =
          pull.foldM[R, E, Boolean](
            _.fold[ZIO[R, E, Boolean]](ZIO.succeed(false))(ZIO.fail(_)), { msg =>
              Tagged.read[NeighborProtocol](msg).map {
                case Accept => true
                case Reject => false
              }
            }
          )
        continue.map(if (_) contStream(ZStream.fromPull(pull)) else ZStream.empty)
      }
    }

}
