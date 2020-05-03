package zio.keeper.transport

import java.math.BigInteger

import zio.keeper.TransportError
import zio.keeper.TransportError.ExceptionWrapper
import zio.{ Chunk, IO, _ }

/**
 * Represents incoming connection.
 *
 * @param read0 - reads Chunk from underlying transport
 * @param write0 - writes Chunk from underlying transport
 * @param isOpen - checks if underlying transport is still available.
 * @param finalizer - finalizer to underlying transport.
 */
final class Channel(
  val read0: Int => IO[TransportError, Chunk[Byte]],
  val write0: Chunk[Byte] => IO[TransportError, Unit],
  val isOpen: IO[TransportError, Boolean],
  val finalizer: IO[TransportError, Unit]
) {

  final val read: IO[TransportError, Chunk[Byte]] =
    for {
      length <- read0(4)
                 .flatMap(
                   c =>
                     ZIO
                       .effect(new BigInteger(c.toArray).intValue())
                       .mapError(ExceptionWrapper)
                 )
      data <- read0(length)
    } yield data

  final def send(data: Chunk[Byte]): IO[TransportError, Unit] = {
    val size = data.size
    write0(
      Chunk((size >>> 24).toByte, (size >>> 16).toByte, (size >>> 8).toByte, size.toByte) ++ data
    )
  }

  final val close: IO[TransportError, Unit] =
    finalizer.ignore
}

object Channel {

  /**
   * Creates synchronized Connection on read and write.
   */
  def withLock(
    read: Int => IO[TransportError, Chunk[Byte]],
    write: Chunk[Byte] => IO[TransportError, Unit],
    isOpen: IO[TransportError, Boolean],
    finalizer: IO[TransportError, Unit]
  ) =
    for {
      writeLock <- Semaphore.make(1)
      readLock  <- Semaphore.make(1)
    } yield {
      new Channel(
        bytes => readLock.withPermit(read(bytes)),
        chunk => writeLock.withPermit(write(chunk)),
        isOpen,
        finalizer
      )
    }
}
