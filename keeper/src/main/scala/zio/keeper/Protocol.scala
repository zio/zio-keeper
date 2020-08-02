package zio.keeper

import zio._
import zio.keeper.transport.Connection

trait Protocol[-R, +E, -I, +O] { self =>

  def step(in: I): ZIO[R, E, (Chunk[O], Option[Protocol[R, E, I, O]])]

  def ++[R1 <: R, E1 >: E, I1 <: I, O1 >: O](that: Protocol[R1, E1, I1, O1]): Protocol[R1, E1, I1, O1] =
    self andThen that

  def andThen[R1 <: R, E1 >: E, I1 <: I, O1 >: O](that: Protocol[R1, E1, I1, O1]): Protocol[R1, E1, I1, O1] =
    new Protocol[R1, E1, I1, O1] {
      def step(in: I1): ZIO[R1,E1,(Chunk[O1], Option[Protocol[R1,E1,I1,O1]])] =
        self.step(in).map { case (out, next) =>
          (out, Some(next.fold(that)(_ andThen that)))
        }
    }

  def biMap[I1, O1](f: I1 => I, g: O => O1): Protocol[R, E, I1, O1] =
    self.contraMap(f).map(g)

  def biMapM[R1 <: R, E1 >: E, I1, O1](f: I1 => ZIO[R1, E1, I], g: O => ZIO[R1, E1, O1]): Protocol[R1, E1, I1, O1] =
    self.contraMapM(f).mapM(g)

  def contraMap[I1](f: I1 => I): Protocol[R, E, I1, O] =
    new Protocol[R, E, I1, O] {
      def step(in: I1): ZIO[R,E,(Chunk[O], Option[Protocol[R,E,I1,O]])] =
        self.step(f(in)).map { case (out, next) => (out, next.map(_.contraMap(f))) }
    }

  def contraMapM[R1 <: R, E1 >: E, I1](f: I1 => ZIO[R1, E1, I]): Protocol[R1, E1, I1, O] =
    new Protocol[R1, E1, I1, O] {
      def step(in: I1): ZIO[R1, E1, (Chunk[O], Option[Protocol[R1,E1,I1,O]])] =
        f(in).flatMap(self.step(_).map { case (out, next) => (out, next.map(_.contraMapM(f))) })
    }

  def forever: Protocol[R, E, I, O] =
    self andThen self.forever

  def map[O1](f: O => O1): Protocol[R, E, I, O1] =
    new Protocol[R, E, I, O1] {
      def step(in: I): ZIO[R,E,(Chunk[O1], Option[Protocol[R,E,I,O1]])] =
        self.step(in).map { case (out, next) => (out.map(f), next.map(_.map(f))) }
    }

  def mapM[R1 <: R, E1 >: E, O1](f: O => ZIO[R1, E1, O1]): Protocol[R1, E1, I, O1] =
    new Protocol[R1, E1, I, O1] {
      def step(in: I): ZIO[R1,E1,(Chunk[O1], Option[Protocol[R1,E1,I,O1]])] =
        self.step(in).flatMap { case (out, next) => ZIO.foreach(out)(f).map((_, next.map(_.mapM(f)))) }
    }

}

object Protocol {

  val end: Protocol[Any, Nothing, Any, Nothing] =
    fromFunction(_ => (Chunk.empty, None))

  def fromEffect[R, E, I, O](f: I => ZIO[R, E, (Chunk[O], Option[Protocol[R, E, I, O]])]): Protocol[R, E, I, O] =
    new Protocol[R, E, I, O] {
      def step(in: I): ZIO[R,E,(Chunk[O], Option[Protocol[R,E,I,O]])] =
        f(in)

    }

  def fromFunction[R, E, I, O](f: I => (Chunk[O], Option[Protocol[R, E, I, O]])): Protocol[R, E, I, O] =
    new Protocol[R, E, I, O] {
      def step(in: I): ZIO[R,E,(Chunk[O], Option[Protocol[R,E,I,O]])] =
        ZIO.succeedNow(f(in))

    }

  def run[R, E, I, O](con: Connection[R, E, O, I], initial: Protocol[R, E, I, O]): ZIO[R, E, Unit] =
    con.receive.foldWhileM(Option(initial))(_.isDefined) {
      case (Some(protocol), message) =>
        protocol.step(message).flatMap { case (out, next) =>
          ZIO.foreach(out)(con.send).as(next)
        }
      case _ =>
        ZIO.dieMessage("impossible")
    }.unit

}
