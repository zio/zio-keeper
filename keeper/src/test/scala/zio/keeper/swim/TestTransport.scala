package zio.keeper.swim

import zio._
import zio.keeper.{ ByteCodec, TransportError }
import zio.keeper.swim.Messages.WithPiggyback
import zio.keeper.transport._
import zio.nio.core.SocketAddress
import zio.stream._

class TestTransport(in: Queue[WithPiggyback], out: Queue[WithPiggyback]) extends ConnectionLessTransport.Service {

  override def bind(
    localAddr: SocketAddress
  )(connectionHandler: Channel => zio.UIO[Unit]): zio.Managed[TransportError, Bind] =
    ZStream
      .fromQueue(in)
      .mapM(ByteCodec[WithPiggyback].toChunk)
      .foreach(chunk => {
        val size          = chunk.size
        var chunkWithSize = Chunk((size >>> 24).toByte, (size >>> 16).toByte, (size >>> 8).toByte, size.toByte) ++ chunk
        val read = (size: Int) => {
          val bytes = chunkWithSize.take(size)
          chunkWithSize = chunkWithSize.drop(size)
          ZIO.succeedNow(bytes)
        }
        connectionHandler(new Channel(read, _ => ZIO.unit, ZIO.succeed(true), ZIO.unit))

      })
      .fork
      .as(new Bind(in.isShutdown, in.shutdown, ZIO.succeed(localAddr)))
      .toManaged(_.close.ignore)

  override def connect(to: SocketAddress): zio.Managed[TransportError, Channel] =
    ZManaged.succeed(
      new Channel(
        _ => ZIO.succeed(Chunk.empty),
        chunk => ByteCodec[WithPiggyback].fromChunk(chunk.drop(4)).flatMap(out.offer).ignore,
        ZIO.succeed(true),
        ZIO.unit
      )
    )

  def incommingMessage(msg: WithPiggyback) = in.offer(msg)
  def outgoingMessages                     = ZStream.fromQueue(out)

}

object TestTransport {

  def make =
    for {
      in  <- Queue.bounded[WithPiggyback](100).toManaged(_.shutdown)
      out <- Queue.bounded[WithPiggyback](100).toManaged(_.shutdown)
    } yield new TestTransport(in, out)

}
