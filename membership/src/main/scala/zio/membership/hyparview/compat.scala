package zio.membership.hyparview

import zio._
import zio.stream._
import zio.stream.ZStream.Pull

// remove with next zio version
object compat {

  implicit class ZStreamSyntax[R, E, A](stream: ZStream[R, E, A]) {

    /**
     * Interrupts the evaluation of this stream when the provided promise resolves. This
     * combinator will also interrupt any in-progress element being pulled from upstream.
     *
     * If the promise completes with a failure, the stream will emit that failure.
     */
    final def interruptWhen[E1 >: E](p: Promise[E1, _]): ZStream[R, E1, A] =
      ZStream {
        for {
          as   <- stream.process
          done <- Ref.make(false).toManaged_
          pull = done.get.flatMap {
            if (_) Pull.end
            else
              as.raceAttempt(
                p.await
                  .mapError(Some(_))
                  .foldCauseM(
                    c => done.set(true) *> ZIO.halt(c),
                    _ => done.set(true) *> Pull.end
                  )
              )
          }
        } yield pull
      }

    /**
     * Drains the provided stream in the background for as long as this stream is running.
     * If this stream ends before `other`, `other` will be interrupted. If `other` fails,
     * this stream will fail with that error.
     */
    final def drainFork[R1 <: R, E1 >: E](other: ZStream[R1, E1, Any]): ZStream[R1, E1, A] =
      ZStream.fromEffect(Promise.make[E1, Nothing]).flatMap { bgDied =>
        ZStream
          .managed(other.foreachManaged(_ => ZIO.unit).catchAllCause(bgDied.halt(_).toManaged_).fork) *>
          stream.interruptWhen(bgDied)
      }

  }

}
