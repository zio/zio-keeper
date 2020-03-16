package zio.keeper.transport

import zio.{ Chunk, IO }
import zio.keeper.TransportError

sealed trait Channel {
  def close: IO[TransportError, Unit]
  def isOpen: IO[TransportError, Boolean]
}

trait ChannelIn extends Channel

trait ChannelOut extends Channel {
  def read: IO[TransportError, Chunk[Byte]]
  def send(data: Chunk[Byte]): IO[TransportError, Unit]
}
