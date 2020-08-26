package zio.keeper.transport

import zio._
import zio.clock.Clock
import zio.duration._
import zio.stream.ZStream
import zio.logging.log
import zio.logging.Logging

trait Connection[-R, +E, -I, +O] { self =>
  def send(data: I): ZIO[R, E, Unit]
  val receive: ZStream[R, E, O]

  def batchInput[R1 <: R with Clock with Logging, I1](
    f: Chunk[I1] => I,
    maxItems: Int = 32,
    batchTimeout: Duration = 100.millis,
    messageBuffer: Int = 256
  ): ZManaged[R1, Nothing, Connection[R1, E, I1, O]] =
    batchInputM(is => ZIO.succeedNow(f(is)), maxItems, batchTimeout, messageBuffer)

  def batchInputM[R1 <: R with Clock with Logging, E1 >: E, I1](
    f: Chunk[I1] => ZIO[R1, E1, I],
    maxItems: Int = 32,
    batchTimeout: Duration = 100.millis,
    messageBuffer: Int = 256
  ): ZManaged[R1, Nothing, Connection[R1, E1, I1, O]] =
    ZQueue.bounded[I1](messageBuffer).toManaged(_.shutdown).flatMap { queue =>
      ZStream
        .fromQueue(queue)
        .groupedWithin(maxItems.toLong, batchTimeout)
        .mapM(
          l =>
            f(Chunk.fromIterable(l))
              .flatMap(self.send)
              .catchAll(e => log.error("Failed sending batched message.", Cause.fail(e)))
        )
        .runDrain
        .toManaged_
        .fork
        .as {
          new Connection[R1, E1, I1, O] {
            def send(data: I1): ZIO[R1, E1, Unit] =
              queue.offer(data).unit

            val receive: ZStream[R, E, O] =
              self.receive

            val close: UIO[Unit] =
              self.close
          }
        }
    }

  def biMap[I1, O1](f: I1 => I, g: O => O1): Connection[R, E, I1, O1] =
    new Connection[R, E, I1, O1] {
      def send(data: I1): ZIO[R, E, Unit] = self.send(f(data))

      val receive: ZStream[R, E, O1] = self.receive.map(g)

      val close: UIO[Unit] = self.close

    }

  def biMapM[R1 <: R, E1 >: E, I1, O1](f: I1 => ZIO[R1, E1, I], g: O => ZIO[R1, E1, O1]): Connection[R1, E1, I1, O1] =
    new Connection[R1, E1, I1, O1] {
      def send(data: I1): ZIO[R1, E1, Unit] = f(data).flatMap(self.send)

      val receive: ZStream[R1, E1, O1] = self.receive.mapM(g)

      val close: UIO[Unit] = self.close

    }

  def contraMap[I1](f: I1 => I): Connection[R, E, I1, O] =
    new Connection[R, E, I1, O] {
      def send(data: I1): ZIO[R, E, Unit] = self.send(f(data))

      val receive: ZStream[R, E, O] = self.receive

      val close: UIO[Unit] = self.close

    }

  def contraMapM[R1 <: R, E1 >: E, I1](f: I1 => ZIO[R1, E1, I]): Connection[R1, E1, I1, O] =
    new Connection[R1, E1, I1, O] {
      def send(data: I1): ZIO[R1, E1, Unit] = f(data).flatMap(self.send)

      val receive: ZStream[R1, E1, O] = self.receive

      val close: UIO[Unit] = self.close

    }

  def map[O1](f: O => O1): Connection[R, E, I, O1] =
    new Connection[R, E, I, O1] {

      def send(data: I): ZIO[R, E, Unit] =
        self.send(data)

      val receive: ZStream[R, E, O1] =
        self.receive.map(f)

      val close: UIO[Unit] =
        self.close

    }

  def mapError[E1](f: E => E1): Connection[R, E1, I, O] =
    new Connection[R, E1, I, O] {

      def send(data: I): ZIO[R, E1, Unit] =
        self.send(data).mapError(f)

      val receive: ZStream[R, E1, O] =
        self.receive.mapError(f)

      val close: UIO[Unit] =
        self.close

    }

  def mapM[R1 <: R, E1 >: E, O1](f: O => ZIO[R1, E1, O1]): Connection[R1, E1, I, O1] =
    new Connection[R1, E1, I, O1] {

      def send(data: I): ZIO[R1, E1, Unit] =
        self.send(data)

      val receive: ZStream[R1, E1, O1] =
        self.receive.mapM(f)

      val close: UIO[Unit] =
        self.close

    }

  def tapOut[R1 <: R, E1 >: E](f: O => ZIO[R1, E1, _]): Connection[R1, E1, I, O] =
    mapM(o => f(o).as(o))

  def tapIn[R1 <: R, E1 >: E, I1 <: I](f: I1 => ZIO[R1, E1, _]): Connection[R1, E1, I1, O] =
    contraMapM(i => f(i).as(i))

  def unbatchOutput[O1](f: O => Chunk[O1]): Connection[R, E, I, O1] =
    unbatchOutputM(o => ZIO.succeedNow(f(o)))

  def unbatchOutputM[R1 <: R, E1 >: E, O1](f: O => ZIO[R1, E1, Chunk[O1]]): Connection[R1, E1, I, O1] =
    new Connection[R1, E1, I, O1] {

      def send(data: I): ZIO[R1, E1, Unit] =
        self.send(data)

      val receive: ZStream[R1, E1, O1] =
        self.receive.mapM(f(_)).flattenChunks

      val close: UIO[Unit] =
        self.close

    }

  // todo: remove
  val close: UIO[Unit]
}
