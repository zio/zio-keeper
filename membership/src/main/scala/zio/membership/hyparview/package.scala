package zio.membership

import zio.membership.transport.ChunkConnection
import zio._
import zio.stm._

package object hyparview {

  private[hyparview] def addConnection[T](
    to: T,
    con: ChunkConnection
  )(
    implicit
    ev1: Tagged[Protocol[T]],
    ev2: Tagged[InitialProtocol[T]]
  ) =
    for {
      dropped <- Env.using[T](_.addNodeToActiveView(to, con).commit)
      _       <- ZIO.foreach(dropped)(disconnect(_))
      _       <- Protocol.handleProtocol(con)
    } yield ()

  private[hyparview] def send[T, M <: Protocol[T]: Tagged](
    to: T,
    msg: M
  )(
    implicit
    ev: Tagged[Protocol[T]]
  ) =
    Tagged
      .write[M](msg)
      .foldM(
        e => console.putStrLn(s"Failed serializing message. Not sending: $e"),
        chunk =>
          for {
            con <- Env.using[T](_.activeView.get(to).commit)
            _   <- ZIO.foreach(con)(_.send(chunk).orElse(disconnect(to)))
          } yield ()
      )

  private[hyparview] def disconnect[T](
    node: T,
    shutDown: Boolean = false
  )(
    implicit
    ev: Tagged[Protocol[T]]
  ) =
    Env
      .using[T] { env =>
        STM.atomically {
          for {
            conOpt <- env.activeView.get(node)
            task <- conOpt match {
                     case Some(con) =>
                       for {
                         _ <- env.activeView.delete(node)
                         _ <- env.addNodeToPassiveView(node)
                       } yield {
                         for {
                           _ <- Tagged
                                 .write[Protocol[T]](Protocol.Disconnect(env.myself, shutDown))
                                 .flatMap(con.send)
                                 .ignore
                                 .unit
                           _ <- con.close
                         } yield ()
                       }
                     case None => STM.succeed(ZIO.unit)
                   }
          } yield task
        }
      }
      .flatten

  private[hyparview] def report[T] =
    Env
      .using[T] { env =>
        STM.atomically {
          for {
            active  <- env.activeView.keys.map(_.size)
            passive <- env.passiveView.size
          } yield console.putStrLn(s"${env.myself}: { active: $active, passive: $passive }")
        }
      }
      .flatten

}
