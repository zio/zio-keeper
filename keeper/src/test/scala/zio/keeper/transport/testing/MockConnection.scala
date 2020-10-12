package zio.keeper.transport.testing

import zio._
import zio.stream.Take
import zio.stream.ZStream
import zio.test.{ Assertion, TestResult, assert }
import zio.keeper.transport.Connection

sealed trait MockConnection[+E, -I, +O] { self =>
  import MockConnection._

  def ++ [E1 >: E, I1 <: I, O1 >: O](that: MockConnection[E1, I1, O1]): MockConnection[E1, I1, O1] =
    self.andThen(that)

  def andThen[E1 >: E, I1 <: I, O1 >: O](
    that: MockConnection[E1, I1, O1]
  ): MockConnection[E1, I1, O1] =
    AndThen(self, that)

  lazy val render: String = self match {
    case AndThen(first, second) => s"(${first.render}) ++ (${second.render})"
    case Await(await)           => s"await(${await.render})"
    case EmitChunk(chunk)       => s"emit(${chunk.toString()})"
    case Fail(e)                => s"fail(${e.toString()})"
  }

  def repeat(n: Int): MockConnection[E, I, O] = {
    def go(n: Int, acc: MockConnection[E, I, O]): MockConnection[E, I, O] =
      if (n <= 0) acc
      else (go(n - 1, acc ++ self))
    go(n, self)
  }

  // run (runEmits, runOneAwait, runEmits) and compose results
  def run(in: I): (Chunk[O], ValidationResult, Either[Option[E], MockConnection[E, I, O]]) = {
    val vEmpty          = ValidationResult.empty
    val (out1, result1) = runEmits
    result1.fold[(Chunk[O], ValidationResult, Either[Option[E], MockConnection[E, I, O]])](
      _.fold((out1, vEmpty, Left[Option[E], Nothing](None)))(e => (out1, vEmpty, Left(Some(e)))),
      next1 => {
        val (v, result2) = next1.runOneAwait(in)
        result2.fold[(Chunk[O], ValidationResult, Either[Option[E], MockConnection[E, I, O]])]((out1, v, Left(None))) {
          next2 =>
            val (out2, result3) = next2.runEmits
            val out             = out1 ++ out2
            result3.fold[(Chunk[O], ValidationResult, Either[Option[E], MockConnection[E, I, O]])](
              _.fold((out, v, Left[Option[E], Nothing](None)))(e => (out, v, Left(Some(e)))),
              next3 => (out, v, Right(next3))
            )
        }
      }
    )
  }

  def use[R, E1 >: E](f: Connection[Any, E, I, O] => ZIO[R, E1, TestResult]): ZIO[R, E1, TestResult] =
    Ref.make[ValidationResult](ValidationResult.empty).flatMap { resultRef =>
      val connection = for {
        outbound <- Queue.unbounded[Take[E, O]].toManaged(_.shutdown)
        initial <- {
          val (out, result) = runEmits
          outbound.offer(Take.chunk(out)) *>
            result.fold(
              _.fold(outbound.offer(Take.end))(e => outbound.offer(Take.fail(e))).as(None),
              next => ZIO.succeedNow(Some(next))
            )
        }.toManaged_
        stateRef <- Ref
                     .make(initial)
                     .toManaged(
                       _.get.flatMap(
                         _.fold(ZIO.unit)(
                           s =>
                             resultRef.update(
                               _.add(assert(s"Not entire script consumed. Remainder: ${s.render}")(Assertion.nothing))
                             )
                         )
                       )
                     )
      } yield new Connection[Any, E, I, O] {

        def send(data: I): ZIO[Any, E, Unit] =
          stateRef
            .modify {
              case None =>
                (
                  resultRef
                    .update(_.add(assert(s"Unexpected input: ${data.toString()}")(Assertion.anything)))
                    .as((Chunk.empty[O], Right(None))),
                  None
                )
              case Some(script) =>
                val (out, v, result) = script.run(data)
                val next             = result.fold(_ => None, Some(_))
                (resultRef.update(_ ++ v).as((out, result)), next)
            }
            .flatten
            .flatMap {
              case (out, result) =>
                outbound.offer(Take.chunk(out)) *>
                  result.fold(
                    _.fold[IO[E, Unit]](outbound.offer(Take.end).unit)(
                      e => outbound.offer(Take.fail(e)) *> ZIO.fail(e)
                    ),
                    _ => ZIO.unit
                  )
            }

        val receive: ZStream[Any, E, O] =
          ZStream.repeatEffectChunkOption(outbound.take.flatMap(_.done))
      }
      connection.use(f).zipWith(resultRef.get) {
        case (r1, r2) =>
          r2.toTestResult.fold(r1)(r1 && _)
      }
    }

  private lazy val runEmits: (Chunk[O], Either[Option[E], MockConnection[E, I, O]]) =
    self match {
      case AndThen(first, second) =>
        val (out1, next1) = first.runEmits
        next1.fold(
          _.fold({
            val (out2, next2) = second.runEmits
            (out1 ++ out2, next2)
          })(e => (out1, Left(Some(e)))),
          next => (out1, Right(next ++ second))
        )
      case EmitChunk(values) => (values, Left(None))
      case Fail(value)       => (Chunk.empty, Left(Some(value)))
      case script            => (Chunk.empty, Right(script))
    }

  private def runOneAwait(in: I): (ValidationResult, Option[MockConnection[E, I, O]]) =
    self match {
      case AndThen(first, second) =>
        val (v, result) = first.runOneAwait(in)
        val next        = if (v.continue) result.fold(Some(second))(remaining => Some(remaining ++ second)) else None
        (v, next)
      case Await(assertion) =>
        (ValidationResult.of(assert(in)(assertion)), None)
      case script =>
        (ValidationResult.empty, Some(script))
    }
}

object MockConnection {

  final case class AndThen[E, I, O](first: MockConnection[E, I, O], second: MockConnection[E, I, O])
      extends MockConnection[E, I, O]
  final case class Await[I](assertion: Assertion[I]) extends MockConnection[Nothing, I, Nothing]
  final case class EmitChunk[O](values: Chunk[O])    extends MockConnection[Nothing, Any, O]
  final case class Fail[E](value: E)                 extends MockConnection[E, Any, Nothing]

  def await[I](assertion: Assertion[I]): MockConnection[Nothing, I, Nothing] =
    Await(assertion)

  def emit[O](value: O): MockConnection[Nothing, Any, O] =
    emitChunk(Chunk.single(value))

  def emitAll[O](values: O*): MockConnection[Nothing, Any, O] =
    emitChunk(Chunk.fromIterable(values))

  def emitChunk[O](values: Chunk[O]): MockConnection[Nothing, Any, O] =
    EmitChunk(values)

  def fail[E](e: E): MockConnection[E, Any, Nothing] =
    Fail(e)

}
