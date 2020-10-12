package zio.keeper.transport.testing

import zio._
import zio.stream.Take
import zio.stream.ZStream
import zio.test.{ Assertion, TestResult, assert }
import zio.keeper.transport.Connection

object MockConnection {

  sealed trait Script[+E, -I, +V, +O] { self =>
    import Script._

    def ++ [E1 >: E, I1 <: I, V1 >: V, O1 >: O](that: Script[E1, I1, V1, O1]): Script[E1, I1, V1, O1] =
      self.andThen(that)

    def andThen[E1 >: E, I1 <: I, V1 >: V, O1 >: O](that: Script[E1, I1, V1, O1]): Script[E1, I1, V1, O1] =
      AndThen(self, that)

    def mapValidation[V1](f: V => V1): Script[E, I, V1, O] = self match {
      case AndThen(first, second) => first.mapValidation(f) ++ second.mapValidation(f)
      case Await(assertion)       => Await(i => assertion(i).map(f))
      case s @ EmitChunk(_)       => s
      case s @ Fail(_)            => s
    }

    lazy val render: String = self match {
      case AndThen(first, second) => s"(${first.render}) ++ (${second.render})"
      case Await(_)               => "await(???)"
      case EmitChunk(_)           => "emit(???)"
      case Fail(_)                => "fail(???)"
    }

    def repeat(n: Int): Script[E, I, V, O] = {
      def go(n: Int, acc: Script[E, I, V, O]): Script[E, I, V, O] =
        if (n <= 0) acc
        else (go(n - 1, acc ++ self))
      go(n, self)
    }

    def runEmits: (Chunk[O], Either[E, Option[Script[E, I, V, O]]]) =
      self match {
        case AndThen(first, second) =>
          val (out1, next1) = first.runEmits
          next1.fold(
            e => (out1, Left(e)),
            _.fold {
              val (out2, next2) = second.runEmits
              (out1 ++ out2, next2)
            } { next =>
              (out1, Right(Some(next ++ second)))
            }
          )
        case EmitChunk(values) => (values, Right(None))
        case Fail(value)       => (Chunk.empty, Left(value))
        case script            => (Chunk.empty, Right(Some(script)))
      }

    def runOneAwait(in: I): Either[V, Option[Script[E, I, V, O]]] =
      self match {
        case AndThen(first, second) =>
          first.runOneAwait(in).map(_.fold(Some(second))(remaining => Some(remaining ++ second)))
        case Await(assertion) =>
          assertion(in).fold[Either[V, None.type]](Right(None))(Left(_))
        case script =>
          Right(Some(script))
      }

    // run (runEmits, runOneAwait, runEmits) and compose results
    def run(in: I): (Chunk[O], Either[V, Either[Option[E], Script[E, I, V, O]]]) = {
      val (out1, result) = runEmits
      result.fold(
        e => (out1, Right(Left(Some(e)))),
        _.fold[(Chunk[O], Either[V, Either[Option[E], Script[E, I, V, O]]])]((out1, Right(Left(None)))) { next1 =>
          next1
            .runOneAwait(in)
            .fold(
              v1 => (out1, Left(v1)),
              _.fold[(Chunk[O], Either[V, Either[Option[E], Script[E, I, V, O]]])]((out1, Right(Left(None)))) { next2 =>
                val (out2, result2) = next2.runEmits
                result2.fold(
                  e => (out1 ++ out2, Right(Left(Some(e)))),
                  _.fold[(Chunk[O], Either[V, Either[Option[E], Script[E, I, V, O]]])](
                    (out1 ++ out2, Right(Left(None)))
                  )(script => (out1 ++ out2, Right(Right(script))))
                )
              }
            )
        }
      )
    }

    def use[R, E1 >: E, I1 <: I, V1 >: V, A](
      initial: V1,
      combine: (V1, V1) => V1,
      onUnConsumed: Script[E, I1, V1, O] => V1,
      onUnexpected: I1 => V1
    )(f: Connection[Any, E, I1, O] => ZIO[R, E1, A]): ZIO[R, E1, (A, V1)] =
      Ref.make[V1](initial).flatMap { resultRef =>
        val connection = for {
          outbound <- Queue.unbounded[Take[E, O]].toManaged(_.shutdown)
          initial <- {
            val (out, result) = runEmits
            outbound.offer(Take.chunk(out)) *>
              result.fold(
                e => outbound.offer(Take.fail(e)).as(None),
                _.fold[UIO[Option[Script[E, I1, V1, O]]]](outbound.offer(Take.end).as(None))(
                  next => ZIO.succeedNow(Some(next))
                )
              )
          }.toManaged_
          stateRef <- Ref
                       .make(initial)
                       .toManaged(
                         _.get.flatMap(
                           _.fold(ZIO.unit)(
                             s =>
                               resultRef.update(
                                 combine(_, onUnConsumed(s))
                               )
                           )
                         )
                       )
        } yield new Connection[Any, E, I1, O] {

          def send(data: I1): ZIO[Any, E, Unit] =
            stateRef
              .modify {
                case None =>
                  (
                    resultRef
                      .update(combine(_, onUnexpected(data)))
                      .as((Chunk.empty[O], Right(None))),
                    None
                  )
                case Some(script) =>
                  val (out, result) = script.run(data)
                  result.fold(
                    v => (resultRef.update(combine(_, v)).as((out, Right(None))), None),
                    _.fold(
                      _.fold[(UIO[(Chunk[O], Either[Option[E], Unit])], Option[Script[E, I1, V1, O]])](
                        (ZIO.succeedNow((out, Left(None))), None)
                      )(e => (ZIO.succeedNow((out, Left(Some(e)))), None)),
                      script => (ZIO.succeedNow((out, Right(()))), Some(script))
                    )
                  )
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
        connection.use(f) &&& resultRef.get
      }

    def useTest[R, E1 >: E](
      f: Connection[Any, E, I, O] => ZIO[R, E1, TestResult]
    )(implicit ev: V <:< TestResult): ZIO[R, E1, TestResult] = {
      val script: Script[E, I, TestResult, O] = self.mapValidation(ev)
      script
        .use[R, E1, I, TestResult, TestResult](
          assert(())(Assertion.anything),
          _ && _,
          s => assert(s"Not entire script consumed. Remainder: ${s.render}")(Assertion.nothing),
          i => assert(s"Unexpected input: ${i.toString()}")(Assertion.anything)
        )(f)
        .map { case (v1, v2) => v1 && v2 }
    }

  }

  object Script {

    final case class AndThen[E, I, V, O](first: Script[E, I, V, O], second: Script[E, I, V, O])
        extends Script[E, I, V, O]
    final case class Await[I, V](assertion: I => Option[V]) extends Script[Nothing, I, V, Nothing]
    final case class EmitChunk[O](values: Chunk[O])         extends Script[Nothing, Any, Nothing, O]
    final case class Fail[E](value: E)                      extends Script[E, Any, Nothing, Nothing]
  }

  def await[I](assertion: Assertion[I]): Script[Nothing, I, TestResult, Nothing] = {
    def f = (in: I) => {
      val result = assert(in)(assertion)
      if (result.isSuccess) None
      else Some(result)
    }
    Script.Await(f)
  }

  def emit[O](value: O): Script[Nothing, Any, Nothing, O] =
    emitChunk(Chunk.single(value))

  def emitAll[O](values: O*): Script[Nothing, Any, Nothing, O] =
    emitChunk(Chunk.fromIterable(values))

  def emitChunk[O](values: Chunk[O]): Script[Nothing, Any, Nothing, O] =
    Script.EmitChunk(values)

  def fail[E](e: E): Script[E, Any, Nothing, Nothing] =
    Script.Fail(e)

  def use[R, E, I, O](
    script: Script[E, I, TestResult, O]
  )(f: Connection[Any, E, I, O] => ZIO[R, E, TestResult]): ZIO[R, E, TestResult] =
    Ref.make[TestResult](assert(())(Assertion.anything)).flatMap { resultRef =>
      val connection = for {
        outbound <- Queue.unbounded[Take[E, O]].toManaged(_.shutdown)
        initial <- {
          val (out, result) = script.runEmits
          outbound.offer(Take.chunk(out)) *>
            result.fold(
              e => outbound.offer(Take.fail(e)).as(None),
              _.fold[UIO[Option[Script[E, I, TestResult, O]]]](outbound.offer(Take.end).as(None))(
                next => ZIO.succeedNow(Some(next))
              )
            )
        }.toManaged_
        stateRef <- Ref
                     .make(initial)
                     .toManaged(
                       _.get.flatMap(
                         _.fold(ZIO.unit)(
                           s =>
                             resultRef.update(
                               _ && assert(s"Not entire script consumed. Remainder: ${s.render}")(Assertion.nothing)
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
                    .update(_ && assert(s"Unexpected message received ${data.toString}")(Assertion.anything))
                    .as((Chunk.empty[O], Right(None))),
                  None
                )
              case Some(script) =>
                val (out, result) = script.run(data)
                result.fold(
                  v => (resultRef.update(_ && v).as((out, Right(None))), None),
                  _.fold(
                    _.fold[(UIO[(Chunk[O], Either[Option[E], Unit])], Option[Script[E, I, TestResult, O]])](
                      (ZIO.succeedNow((out, Left(None))), None)
                    )(e => (ZIO.succeedNow((out, Left(Some(e)))), None)),
                    script => (ZIO.succeedNow((out, Right(()))), Some(script))
                  )
                )
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
      connection.use(f).flatMap(first => resultRef.get.map(first && _))
    }
  // for {
  //   outbound <- Queue.unbounded[Take[E, O]].toManaged(_.shutdown)
  //   initial <- {
  //     val (out, result) = script.runEmits
  //     outbound.offer(Take.chunk(out)) *>
  //       result.fold(
  //         e => outbound.offer(Take.fail(e)).as(None),
  //         _.fold[UIO[Option[Script[E, I, O]]]](outbound.offer(Take.end).as(None))(next => ZIO.succeedNow(Some(next)))
  //       )
  //   }.toManaged_
  //   stateRef <- Ref
  //                .make(initial)
  //                .toManaged(
  //                  _.get.flatMap(
  //                    _.fold(ZIO.unit)(s => ZIO.dieMessage(s"Not entire script consumed. Remainder: ${s.render}"))
  //                  )
  //                )
  // } yield new Connection[Any, E, I, O] {

  //   def send(data: I): ZIO[Any, E, Unit] =
  //     stateRef
  //       .modify {
  //         case None => (ZIO.dieMessage(s"unexpected message received ${data.toString}"), None)
  //         case Some(script) =>
  //           val (out, result) = script.run(data)
  //           (ZIO.succeedNow((out, result)), result.toOption.flatten)
  //       }
  //       .flatten
  //       .flatMap {
  //         case (out, result) =>
  //           outbound.offer(Take.chunk(out)) *>
  //             result.fold(
  //               e => outbound.offer(Take.fail(e)) *> ZIO.fail(e),
  //               _.fold(outbound.offer(Take.end).unit)(_ => ZIO.unit)
  //             )
  //       }

  //   val receive: ZStream[Any, E, O] =
  //     ZStream.repeatEffectChunkOption(outbound.take.flatMap(_.done))
  // }

  // def make[E, I, O](script: Script[E, I, O]): Managed[Nothing, Connection[Any, E, I, O]] =
  //   for {
  //     outbound <- Queue.unbounded[Take[E, O]].toManaged(_.shutdown)
  //     initial <- {
  //       val (out, result) = script.runEmits
  //       outbound.offer(Take.chunk(out)) *>
  //         result.fold(
  //           e => outbound.offer(Take.fail(e)).as(None),
  //           _.fold[UIO[Option[Script[E, I, O]]]](outbound.offer(Take.end).as(None))(next => ZIO.succeedNow(Some(next)))
  //         )
  //     }.toManaged_
  //     stateRef <- Ref
  //                  .make(initial)
  //                  .toManaged(
  //                    _.get.flatMap(
  //                      _.fold(ZIO.unit)(s => ZIO.dieMessage(s"Not entire script consumed. Remainder: ${s.render}"))
  //                    )
  //                  )
  //   } yield new Connection[Any, E, I, O] {

  //     def send(data: I): ZIO[Any, E, Unit] =
  //       stateRef
  //         .modify {
  //           case None => (ZIO.dieMessage(s"unexpected message received ${data.toString}"), None)
  //           case Some(script) =>
  //             val (out, result) = script.run(data)
  //             (ZIO.succeedNow((out, result)), result.toOption.flatten)
  //         }
  //         .flatten
  //         .flatMap {
  //           case (out, result) =>
  //             outbound.offer(Take.chunk(out)) *>
  //               result.fold(
  //                 e => outbound.offer(Take.fail(e)) *> ZIO.fail(e),
  //                 _.fold(outbound.offer(Take.end).unit)(_ => ZIO.unit)
  //               )
  //         }

  //     val receive: ZStream[Any, E, O] =
  //       ZStream.repeatEffectChunkOption(outbound.take.flatMap(_.done))

  //   }
}
