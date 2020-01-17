package zio.membership.hyparview

import zio.{ Exit, Ref, Reservation, UIO, ZIO, ZManaged }

trait ScopeIO {
  def apply[R, E, A](managed: ZManaged[R, E, A]): ZIO[R, E, (A, UIO[_])]
}

object ScopeIO {

  def make: ZManaged[Any, Nothing, ScopeIO] =
    ZManaged {
      // we abuse the fact that Function1 will use reference equality
      Ref.make(Set.empty[Exit[Any, Any] => ZIO[Any, Nothing, Any]]).map { finalizers =>
        Reservation(
          acquire = ZIO.succeed {
            new ScopeIO {
              override def apply[R, E, A](managed: ZManaged[R, E, A]) =
                ZIO.uninterruptibleMask { restore =>
                  for {
                    env      <- ZIO.environment[R]
                    res      <- managed.reserve
                    resource <- restore(res.acquire).onError(err => res.release(Exit.Failure(err)))
                    release  = res.release.andThen(_.provide(env))
                    _        <- finalizers.update(_ + release)
                    done <- Ref
                             .make(release(Exit.Success(resource)).ensuring(finalizers.update(_ - release)))
                             .map(_.modify(old => (old, ZIO.unit)).flatten)
                  } yield (resource, done)
                }
            }
          },
          release = exitU =>
            for {
              fs    <- finalizers.get
              exits <- ZIO.foreachPar(fs)(_(exitU).run)
              _     <- ZIO.done(Exit.collectAllPar(exits).getOrElse(Exit.unit))
            } yield ()
        )
      }
    }

}
