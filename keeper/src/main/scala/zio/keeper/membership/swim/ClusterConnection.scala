package zio.keeper.membership.swim

import zio.{Chunk, ZIO}
import zio.keeper.Message._
import zio.keeper.transport.Connection
import zio.keeper.{ByteCodec, Error, Message}

private [swim] class ClusterConnection[A: ByteCodec](tConn: Connection[A]) {
  def read: ZIO[Any, Error, Message] =
    readMessage(tConn)

  def send(data: Chunk[Byte]): ZIO[Any, Error, Unit] =
    serializeMessage(tConn.address, data, 1)>>= tConn.send

  def close: ZIO[Any, Error, Unit] =
    tConn.close

  def address: A  =
    tConn.address
}


