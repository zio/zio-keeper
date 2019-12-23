package zio.membership.hyparview

import zio.membership.ByteCodec
import upickle.default._
import zio.membership.ByteCodec
import upickle.default._
import zio._
import zio.membership.Error
import zio.stream.ZStream

final case class JoinReply[T] (
  remote: T
)

object JoinReply {

  implicit def codec[T: ReadWriter]: ByteCodec[JoinReply[T]] =
    ByteCodec.fromReadWriter(macroRW[JoinReply[T]])

  def receive[R, R1 <: R, E >: Error, E1 >: E, T, A](
    stream: ZStream[R, E, Chunk[Byte]]
  )(
    contStream: (T, ZStream[R, E, Chunk[Byte]]) => ZStream[R1, E1, A]
  )(
    implicit ev: ByteCodec[JoinReply[T]]
  ): ZStream[R1, E1, A] =
    ZStream.unwrapManaged {
      stream.process.mapM { pull =>
        val continue =
          pull.foldM[R, E, Option[T]](
            _.fold[ZIO[R, E, Option[T]]](ZIO.succeed(None))(ZIO.fail(_)), { msg =>
              ByteCodec[JoinReply[T]].fromChunk(msg).map {
                case JoinReply(addr) => Some(addr)
              }
            }
          )
        continue.map(_.fold[ZStream[R1, E1, A]](ZStream.empty)(contStream(_, ZStream.fromPull(pull))))
      }
    }
}
