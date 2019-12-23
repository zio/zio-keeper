package zio.membership

import zio._

package object hyparview {

  private[hyparview] def send[T](
    to: T,
    msg: ActiveProtocol[T]
  ): ZIO[Env[T], Nothing, Unit] =
    for {
      con <- Env.using[T](_.activeView.get(to).commit)
      _   <- ZIO.foreach(con)(_(msg))
    } yield ()

  private[hyparview] def sendInitial[T](
    to: T,
    msg: InitialProtocol[T]
  ): ZIO[Env[T], Nothing, Unit] =
    Env.using[T](_.sendInitial(to, msg))

  private[hyparview] def disconnect[T](
    node: T,
    shutDown: Boolean = false
  ): ZIO[Env[T], Nothing, Unit] =
    Env.using[T](e => ZIO.succeed(e.myself)).flatMap(addr => send(node, ActiveProtocol.Disconnect(addr, shutDown)))

}
