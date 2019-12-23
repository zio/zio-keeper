package zio.membership.hyparview

import zio._

sealed trait Command

object Command {

  final case class Send(data: Chunk[Byte])       extends Command
  final case class Disconnect(shutDown: Boolean) extends Command

}
