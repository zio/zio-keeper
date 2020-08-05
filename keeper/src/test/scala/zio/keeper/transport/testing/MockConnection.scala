package zio.keeper.transport.testing

import zio._
import zio.stream.ZStream
import zio.test.Assertion
import zio.keeper.transport.Connection

object MockConnection {

  sealed trait Operation[-I, +O]

  final case class Await[A](assertion: Assertion[A])         extends Operation[A, Nothing]
  final case class Emit[A](value: A)                         extends Operation[Any, A]
  final case class Transform[A, B](f: PartialFunction[A, B]) extends Operation[A, B]

  def await[A](assertion: Assertion[A]): Operation[A, Nothing] =
    Await(assertion)

  def emit[A](value: A): Operation[Any, A] =
    Emit(value)

  def transfrom[A, B](f: PartialFunction[A, B]): Operation[A, B] =
    Transform(f)

  def make[A, B](ops: Operation[A, B]*): Managed[Nothing, Connection[Any, Unit, A, B]] =
    Ref.makeManaged(ops.toList).flatMap { ops =>
      Queue.bounded[B](256).toManaged(_.shutdown).mapM { outbound =>
        val emitAll = ops
          .modify(
            xs =>
              xs.span {
                case Emit(_) => true
                case _       => false
              }
          )
          .flatMap {
            ZIO.foreach(_) {
              case Emit(value) =>
                outbound.offer(value)
              case _ =>
                ZIO.unit
            }
          }
          .unit
        val protocol = new Connection[Any, Unit, A, B] {
          def send(data: A): ZIO[Any, Unit, Unit] =
            ops.modify(xs => (xs.headOption, xs.tail)).flatMap {
              case None =>
                ZIO.fail(())
              case Some(Await(assertion)) =>
                if (assertion.run(data).isSuccess) ZIO.unit
                else ZIO.fail(())
              case Some(Transform(f)) =>
                if (f.isDefinedAt(data)) outbound.offer(f(data)).unit
                else ZIO.fail(())
              case _ =>
                ZIO.dieMessage("impossible")
            } *> emitAll

          val receive: ZStream[Any, Nothing, B] =
            ZStream.fromQueue(outbound)

          val close: UIO[Unit] =
            ZIO.unit
        }
        emitAll.as(protocol)
      }
    }
}
