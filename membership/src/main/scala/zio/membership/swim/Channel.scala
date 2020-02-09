package zio.membership.swim

import zio.membership.{ Error, Member, TransportError }
import zio.stream.Stream
import zio.{ Chunk, IO, UIO }


/**
 * Channel represents connection to node in the cluster.
 *
 * @tparam A - type of address
 */
trait Channel[A] {
  def member: Member[A]
  def send(msg: Chunk[Byte]): IO[TransportError, Unit]
  def receive: Stream[Error, Chunk[Byte]]
  def close: UIO[_]
}

object Channel {

  def make[A](
    member0: Member[A],
    send0: Chunk[Byte] => IO[TransportError, Unit],
    receive0: Stream[Error, Chunk[Byte]],
    release0: UIO[_]
  ): Channel[A] =
    new Channel[A] {
      override def member: Member[A] = member0

      override def send(msg: Chunk[Byte]): IO[TransportError, Unit] = send0(msg)

      override def receive: Stream[Error, Chunk[Byte]] = receive0

      override def close: UIO[_] = release0
    }
}
