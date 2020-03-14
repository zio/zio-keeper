package zio.keeper.transport

import java.io.IOException
import java.math.BigInteger

import zio.keeper.TransportError
import zio.keeper.TransportError.ExceptionWrapper
import zio.{ Chunk, IO, _ }

sealed trait Channel {
  def close: IO[TransportError, Unit]
  def isOpen: IO[TransportError, Boolean]
}

object Channel {

  class Connection(
    read0: Int => IO[TransportError, Chunk[Byte]],
    write0: Chunk[Byte] => IO[TransportError, Unit],
    val isOpen: IO[TransportError, Boolean],
    finalizer: IO[TransportError, Unit]
  ) extends Channel {

    def read: IO[TransportError, Chunk[Byte]] =
      (for {
        length <- read0(4)
                   .flatMap(c =>
                     ZIO.effect(new BigInteger(c.toArray).intValue())
                     .mapError(ExceptionWrapper)
                   )

        data <- read0(length)
      } yield data)
        .catchSome {
          case ExceptionWrapper(ex: IOException) if ex.getMessage == "Connection reset by peer" =>
            close *> ZIO.fail(ExceptionWrapper(ex))
        }

    def send(data: Chunk[Byte]): IO[TransportError, Unit] = {
      val size = data.size
      (for {
        _ <- write0(Chunk((size >>> 24).toByte, (size >>> 16).toByte, (size >>> 8).toByte, size.toByte))
        _ <- write0(data).retry(Schedule.recurs(3))
      } yield ())
        .catchSome {
          case ExceptionWrapper(ex: IOException) if ex.getMessage == "Connection reset by peer" =>
            close *> ZIO.fail(ExceptionWrapper(ex))
        }
    }

    def close: IO[TransportError, Unit] =
      finalizer.ignore
  }

  class Bind(val isOpen: IO[TransportError, Boolean], val close: IO[TransportError, Unit]) extends Channel
}
