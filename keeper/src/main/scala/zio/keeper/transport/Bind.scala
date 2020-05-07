package zio.keeper.transport

import zio.IO
import zio.keeper.TransportError
import zio.nio.core.SocketAddress

final class Bind(
  val isOpen: IO[TransportError, Boolean],
  val close: IO[TransportError, Unit],
  val localAddress: IO[TransportError, SocketAddress]
)
