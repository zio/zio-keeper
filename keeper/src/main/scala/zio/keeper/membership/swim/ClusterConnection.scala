package zio.keeper.membership.swim

import zio.keeper.Message._
import zio.keeper.membership.NodeAddress
import zio.keeper.transport.Connection
import zio.keeper.{Error, Message}
import zio.{Chunk, ZIO}

private [swim] class ClusterConnection(tConn: Connection, val address: NodeAddress) {
  def read: ZIO[Any, Error, Message] =
    readMessage(tConn)

  def send(data: Chunk[Byte]): ZIO[Any, Error, Unit] =
    serializeMessage(tConn.address, data, 1)>>= tConn.send

  def close: ZIO[Any, Error, Unit] =
    tConn.close

}


