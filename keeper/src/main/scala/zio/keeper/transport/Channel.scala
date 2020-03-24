package zio.keeper.transport

import java.math.BigInteger

import zio.keeper.TransportError
import zio.keeper.TransportError.ExceptionWrapper
import zio.nio.core.SocketAddress
import zio.{ Chunk, IO, _ }

/**
 * Channel represents connection
 */
sealed trait Channel {

  /**
   * Execute all finalizers from underlying transport and close channel.
   */
  def close: IO[TransportError, Unit]

  /**
   * Checks if channel is still open.
   */
  def isOpen: IO[TransportError, Boolean]
}

object Channel {

  /**
   * Represents incoming connection.
   *
   * @param read0 - reads Chunk from underlying transport
   * @param write0 - writes Chunk from underlying transport
   * @param isOpen - checks if underlying transport is still available.
   * @param finalizer - finalizer to underlying transport.
   */
  class Connection(
    read0: Int => IO[TransportError, Chunk[Byte]],
    write0: Chunk[Byte] => IO[TransportError, Unit],
    val isOpen: IO[TransportError, Boolean],
    finalizer: IO[TransportError, Unit]
  ) extends Channel {

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

  object Connection {

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
        new Connection(
          bytes => readLock.withPermit(read(bytes)),
          chunk => writeLock.withPermit(write(chunk)),
          isOpen,
          finalizer
        )
      }
  }

  /**
   * Represents a local listener.
   *
   * @param isOpen check if underlying transport is still available
   * @param close finalizer for underlying transport
   * @param localAddress returns the locally bound address
   */
  class Bind(
    val isOpen: IO[TransportError, Boolean],
    val close: IO[TransportError, Unit],
    val localAddress: IO[TransportError, SocketAddress]
  ) extends Channel
}
