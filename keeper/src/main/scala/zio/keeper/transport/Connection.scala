package zio.keeper.transport

import zio._
import zio.stream.ZStream

trait Connection[-R, +E, -I, +O] { self =>
  def send(data: I): ZIO[R, E, Unit]
  val receive: ZStream[R, E, O]

  def biMap[I1, O1](f: I1 => I, g: O => O1): Connection[R, E, I1, O1] =
    new Connection[R, E, I1, O1] {
      def send(data: I1): ZIO[R, E, Unit] = self.send(f(data))

      val receive: ZStream[R, E, O1] = self.receive.map(g)

      val close: zio.UIO[Unit] = self.close

    }

  def biMapM[R1 <: R, E1 >: E, I1, O1](f: I1 => ZIO[R1, E1, I], g: O => ZIO[R1, E1, O1]): Connection[R1, E1, I1, O1] =
    new Connection[R1, E1, I1, O1] {
      def send(data: I1): ZIO[R1, E1, Unit] = f(data).flatMap(self.send)

      val receive: ZStream[R1, E1, O1] = self.receive.mapM(g)

      val close: zio.UIO[Unit] = self.close

    }

  def contraMap[I1](f: I1 => I): Connection[R, E, I1, O] =
    new Connection[R, E, I1, O] {
      def send(data: I1): ZIO[R, E, Unit] = self.send(f(data))

      val receive: ZStream[R, E, O] = self.receive

      val close: zio.UIO[Unit] = self.close

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

      val close: zio.UIO[Unit] =
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

  def recoverErrors[R1 <: R, E1, I1 <: I, O1 >: O](
    f: (E, I1) => ZIO[R1, E1, Unit],
    g: E => ZIO[R1, E1, O1]
  ): Connection[R1, E1, I1, O1] =
    new Connection[R1, E1, I1, O1] {

      def send(data: I1): ZIO[R1, E1, Unit] =
        self.send(data).foldM(f(_, data), ZIO.succeedNow)

      // TODO: this is a hack. Switch to proper error handling combinators once ZStream supports them.
      val receive: ZStream[R1, E1, O1] = {
        def go(stream: ZStream[R, E, O]): ZStream[R1, E1, O1] =
          stream.catchAll { e =>
            ZStream.fromEffect(g(e)) ++ go(stream)
          }
        go(self.receive)
      }

      val close: zio.UIO[Unit] =
        self.close

    }

  // todo: remove
  val close: UIO[Unit]
}
