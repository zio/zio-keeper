package zio.membership

import zio._

package object hyparview {
import zio.logging.Logging

  private[hyparview] def send[T](
    to: T,
    msg: ActiveProtocol[T]
  )(
    implicit
    ev: Tagged[ActiveProtocol[T]]
  ): ZIO[Env[T] with Logging[String], Nothing, Unit] =
    Tagged
      .write[ActiveProtocol[T]](msg)
      .foldM(
        e => log.error(s"Failed serializing $msg", Cause.fail(e)),
        chunk =>
          for {
            _     <- log.debug(s"send: $to -> $msg")
            con   <- Env.using[T](_.activeView.get(to).commit)
            _     <- ZIO.foreach(con)(_(Command.Send(chunk)))
          } yield ()
      )

  private[hyparview] def sendInitial[T](
    to: T,
    msg: InitialProtocol[T]
  ): ZIO[Env[T] with Logging[String], Nothing, Unit] =
    log.debug(s"sendInitial: $to -> $msg") *>
    Env.using[T](_.sendInitial(to, msg))

  private[hyparview] def disconnect[T](
    node: T,
    shutDown: Boolean = false
  ) =
    Env.using[T](_.activeView.get(node).commit).flatMap {
      case None          => ZIO.unit
      case Some(sendMsg) => sendMsg(Command.Disconnect(shutDown))
    }

}
