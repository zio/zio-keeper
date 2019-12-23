package zio.membership

import zio._
import zio.stm._

package object hyparview {

  private[hyparview] def send[T](
    to: T,
    msg: ActiveProtocol[T]
  )(
    implicit
    ev: Tagged[ActiveProtocol[T]]
  ) =
    Tagged
      .write[ActiveProtocol[T]](msg)
      .foldM(
        e => console.putStrLn(s"Failed serializing message. Not sending: $e"),
        chunk =>
          for {
            _   <- UIO(println(s"sent: $to -> $msg"))
            con <- Env.using[T](_.activeView.get(to).commit)
            _   <- ZIO.foreach(con)(_(Command.Send(chunk)))
          } yield ()
      )

  private[hyparview] def disconnect[T](
    node: T,
    shutDown: Boolean = false
  ) =
    Env.using[T](_.activeView.get(node).commit).flatMap {
      case None          => ZIO.unit
      case Some(sendMsg) => sendMsg(Command.Disconnect(shutDown))
    }

  private[hyparview] def report[T] =
    Env
      .using[T] { env =>
        STM.atomically {
          for {
            active  <- env.activeView.keys
            passive <- env.passiveView.toList
          } yield console.putStrLn(s"${env.myself}: { active: $active, passive: $passive }")
        }
      }
      .flatten

}
