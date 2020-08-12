package zio.keeper.transport.testing

import zio._
import zio.stream.ZStream
import zio.test.{ AssertResult, Assertion }
import zio.keeper.transport.Connection
import zio.keeper.transport.testing.MockConnection.Script.AndThen
import zio.keeper.transport.testing.MockConnection.Script.Or
import zio.keeper.transport.testing.MockConnection.Script.Await
import zio.keeper.transport.testing.MockConnection.Script.EmitChunk

object MockConnection {

  sealed trait Script[+E, -I, +O] { self =>
    import Script._

    def >>> [E1 >: E, I1 <: I, O1 >: O](that: Script[E1, I1, O1]): Script[E1, I1, O1] =
      AndThen(self, that)

    def || [E1, I1 <: I, O1 >: O](that: Script[E1, I1, O1]): Script[E1, I1, O1] =
      Or(self, that)

    def repeat(n: Int): Script[E, I, O] = {
      def go(n: Int, acc: Script[E, I, O]): Script[E, I, O] =
        if (n <= 0) acc
        else (go(n - 1, acc >>> self))
      go(n, self)
    }

    def runEmits: (Chunk[O], Option[Script[E, I, O]]) =
      self match {
        case AndThen(first, second) =>
          val (out1, next1) = first.runEmits
          next1.fold {
            val (out2, next2) = second.runEmits
            (out1 ++ out2, next2)
          } { next =>
            (out1, Some(next >>> second))
          }
        case Or(first, second) =>
          val (out, next) = first.runEmits
          (out, next.map(_ || second))
        case EmitChunk(values) => (values, None)
        case script            => (Chunk.empty, Some(script))
      }

    def runOneAwait(in: I): Either[E, Option[Script[E, I, O]]] =
      self match {
        case AndThen(first, second) =>
          first.runOneAwait(in).right.map(_.fold(Some(second))(remaining => Some(remaining >>> second)))
        case Or(first, second) =>
          first.runOneAwait(in).fold(_ => second.runOneAwait(in), remaining => Right(remaining.map(_ || second)))
        case Await(assertion) =>
          assertion(in).fold[Either[E, None.type]](Right(None))(Left(_))
        case script =>
          Right(Some(script))
      }

    def run(in: I): (Chunk[O], Either[E, Option[Script[E, I, O]]]) = {
      val (out1, next1) = runEmits
      next1.fold[(Chunk[O], Either[E, Option[Script[E, I, O]]])]((out1, Right(None))) { next1 =>
        next1
          .runOneAwait(in)
          .fold(
            e => (out1, Left(e)),
            _.fold[(Chunk[O], Either[E, Option[Script[E, I, O]]])]((out1, Right(None))) { next2 =>
              val (out2, next3) = next2.runEmits
              (out1 ++ out2, Right(next3))
            }
          )
      }
    }
  }

  object Script {
    final case class AndThen[E, I, O](first: Script[E, I, O], second: Script[E, I, O]) extends Script[E, I, O]
    final case class Or[E, I, O](first: Script[_, I, O], second: Script[E, I, O])      extends Script[E, I, O]
    final case class Await[E, I](assertion: I => Option[E])                            extends Script[E, I, Nothing]
    final case class EmitChunk[O](values: Chunk[O])                                    extends Script[Nothing, Any, O]

    def await[I](assertion: Assertion[I]): Script[AssertResult, I, Nothing] = {
      def f = (in: I) => {
        val result = assertion.run(in)
        if (result.isSuccess) None
        else Some(result)
      }
      Await(f)
    }

    def emit[O](value: O): Script[Nothing, Any, O] =
      emitChunk(Chunk.single(value))

    def emitAll[O](values: O*): Script[Nothing, Any, O] =
      emitChunk(Chunk.fromIterable(values))

    def emitChunk[O](values: Chunk[O]): Script[Nothing, Any, O] =
      EmitChunk(values)
  }

  def make[E, I, O](script: Script[E, I, O]): Managed[Nothing, Connection[Any, E, I, O]] =
    for {
      outbound <- Queue.bounded[O](256).toManaged(_.shutdown)
      initial <- {
        val (out, next) = script.runEmits
        outbound.offerAll(out).as(next)
      }.toManaged_
      stateRef <- Ref.makeManaged(initial)
    } yield new Connection[Any, E, I, O] {

      def send(data: I): ZIO[Any, E, Unit] =
        stateRef
          .modify {
            case None => ((Chunk.empty, None), None)
            case Some(script) =>
              val (out, result) = script.run(data)
              ((out, result.left.toOption), result.right.toOption.flatten)
          }
          .flatMap {
            case (out, error) =>
              outbound.offerAll(out) *> error.fold[IO[E, Unit]](ZIO.unit)(ZIO.fail(_))
          }

      val receive: ZStream[Any, Nothing, O] =
        ZStream.fromQueue(outbound)

      val close: UIO[Unit] =
        ZIO.unit

    }
}
