package zio.membership.hyparview

import zio._
import zio.stm._
import zio.random.Random

object TRandom {

  /**
   * Service that allows to acesss randomness in a STM transaction.
   */
  trait Service {

    /**
     * Pick a random element from a list.
     */
    def selectOne[A](values: List[A]): STM[Nothing, Option[A]]

    /**
     * Pick `n` random elements from a list without replacement.
     */
    def selectN[A](values: List[A], n: Int): STM[Nothing, List[A]]
  }

  def using[R <: TRandom, E, A](f: Service => ZIO[R, E, A]): ZIO[R, E, A] =
    ZIO.environment[TRandom].flatMap(env => f(env.get))

  def live: ZLayer[Random, Nothing, TRandom] =
    ZLayer.fromEffect {
      for {
        seed <- random.nextInt
        tref <- TRef.makeCommit(PRNG.make(seed.toLong))
      } yield new Service {

        private def mod(n: Int, m: Int): Int = {
          val res = n % m
          if (res < 0) res + m else res
        }

        private def pick(n: Int): STM[Nothing, Int] =
          tref.modify(_.next).map(mod(_, n))

        def selectOne[A](values: List[A]): STM[Nothing, Option[A]] =
          if (values.isEmpty) STM.succeed(None)
          else {
            for {
              index    <- pick(values.size)
              selected = values(index)
            } yield Some(selected)
          }

        def selectN[A](values: List[A], n: Int): STM[Nothing, List[A]] = {
          def go(remaining: Vector[A], toPick: Int, acc: List[A]): STM[Nothing, List[A]] =
            (remaining, toPick) match {
              case (Vector(), _) | (_, 0) => STM.succeed(acc)
              case _ =>
                pick(remaining.size).flatMap { index =>
                  val x  = remaining(index)
                  val xs = remaining.patch(index, Nil, 1)
                  go(xs, toPick - 1, x :: acc)
                }
            }
          go(values.toVector, Math.max(0, n), Nil)
        }
      }
    }
}
