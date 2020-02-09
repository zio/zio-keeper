package zio.keeper.membership.swim

import zio.ZIO
import zio.keeper.Message._
import zio.keeper.transport.Connection
import zio.keeper.{Error, Message}

private [swim] class ClusterConnection(tConn: Connection) {
  def read: ZIO[Any, Error, Message] =
    readMessage(tConn).map(_._2)

  def sendInternal(data: Message): ZIO[Any, Error, Unit] =
    serializeMessage(data.sender, data.payload, 1)>>= tConn.send
}

object ClusterConnection {
  def apply(tConn: Connection)(fn: ClusterConnection => ZIO[Any, Error, Unit]): ZIO[Any, Error, ClusterConnection] = {
    val cc = new ClusterConnection(tConn)
    fn(cc).as(cc)
  }
}
