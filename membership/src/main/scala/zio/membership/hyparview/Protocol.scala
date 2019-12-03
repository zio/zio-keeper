package zio.membership.hyparview

import zio._
import zio.membership.transport.ChunkConnection
import zio._
import zio.membership.Error
import zio.membership.ByteCodec
import zio.membership.transport.Transport
import zio.stm._
import zio.console.Console
import zio.console.putStrLn
import upickle.default._

sealed trait Protocol[T]

object Protocol {

  implicit def tagged[T](
    implicit
    c1: ByteCodec[Disconnect[T]],
    c2: ByteCodec[ForwardJoin[T]],
    c3: ByteCodec[Shuffle[T]]
  ): Tagged[Protocol[T]] =
    Tagged.instance(
      {
        case _: Disconnect[T]  => 10
        case _: ForwardJoin[T] => 11
        case _: Shuffle[T]     => 12
      }, {
        case 10 => c1.asInstanceOf[ByteCodec[Protocol[T]]]
        case 11 => c2.asInstanceOf[ByteCodec[Protocol[T]]]
        case 12 => c3.asInstanceOf[ByteCodec[Protocol[T]]]
      }
    )

  final case class Disconnect[T](
    sender: T,
    alive: Boolean
  ) extends Protocol[T]

  object Disconnect {

    implicit def codec[T: ReadWriter]: ByteCodec[Disconnect[T]] =
      ByteCodec.fromReadWriter(macroRW[Disconnect[T]])
  }

  final case class ForwardJoin[T](
    sender: T,
    originalSender: T,
    ttl: TimeToLive
  ) extends Protocol[T]

  object ForwardJoin {

    implicit def codec[T: ReadWriter]: ByteCodec[ForwardJoin[T]] =
      ByteCodec.fromReadWriter(macroRW[ForwardJoin[T]])
  }

  final case class Shuffle[T](
    sender: T,
    originalSender: T,
    activeNodes: List[T],
    passiveNodes: List[T],
    ttl: TimeToLive
  ) extends Protocol[T]

  object Shuffle {

    implicit def codec[T: ReadWriter]: ByteCodec[Shuffle[T]] =
      ByteCodec.fromReadWriter(macroRW[Shuffle[T]])
  }

  private[hyparview] def protocolHandler[T](
    con: ChunkConnection
  )(
    implicit
    ev1: Tagged[Protocol[T]],
    ev2: Tagged[InitialProtocol[T]]
  ): ZIO[Console with Env[T] with Transport[T], Error, Unit] =
    con.receive
      .foreach { msg =>
        Tagged.read[Protocol[T]](msg).flatMap {
          case m: Protocol.Disconnect[T]  => handleDisconnect(m) *> con.close
          case m: Protocol.ForwardJoin[T] => handleForwardJoin(m)
          case m: Protocol.Shuffle[T]     => handleShuffle(m)
        }
      }
      .onError(e => putStrLn(s"Handler failed with ${e}") *> con.close)

  private[hyparview] def handleDisconnect[T](
    msg: Protocol.Disconnect[T]
  ) =
    ZIO.environment[Env[T]].map(_.env).flatMap { env =>
      STM
        .atomically {
          for {
            inActive <- env.activeView.contains(msg.sender)
            _        <- if (inActive) env.activeView.delete(msg.sender) else STM.unit
            _        <- if (inActive && msg.alive) env.addNodeToPassiveView(msg.sender) else STM.unit
          } yield ()
        }
    }

  private[hyparview] def handleForwardJoin[T](
    msg: Protocol.ForwardJoin[T]
  )(
    implicit
    ev1: Tagged[Protocol[T]],
    ev2: Tagged[InitialProtocol[T]]
  ) =
    ZIO.environment[Env[T]].map(_.env).flatMap { env =>
      val accept = for {
        con   <- ZIO.accessM[Transport[T]](_.transport.connect(msg.originalSender))
        reply <- Tagged.write[InitialProtocol[T]](InitialProtocol.ForwardJoinReply(env.myself))
        _     <- con.send(reply)
        _     <- addConnection(msg.originalSender, con)
      } yield ()

      val process = env.activeView.keys.map(ks => (ks.size, msg.ttl.step)).flatMap {
        case (i, _) if i <= 1 =>
          STM.succeed(accept)
        case (_, None) =>
          STM.succeed(accept)
        case (_, Some(ttl)) =>
          for {
            list <- env.activeView.keys.map(_.filterNot(n => n == msg.sender || n == msg.originalSender))
            next <- env.selectOne(list)
            _    <- if (ttl.count == env.cfg.prwl) env.addNodeToPassiveView(msg.originalSender) else STM.unit
          } yield ZIO.foreach(next)(send[T, Protocol[T]](_, msg.copy(sender = env.myself, ttl = ttl)))
      }
      process.commit.flatten
    }

  private[hyparview] def handleShuffle[T](
    msg: Protocol.Shuffle[T]
  )(
    implicit
    ev1: Tagged[InitialProtocol[T]],
    ev2: Tagged[Protocol[T]]
  ) =
    ZIO.environment[Env[T]].map(_.env).flatMap { env =>
      env.activeView.keys
        .map(ks => (ks.size, msg.ttl.step))
        .flatMap {
          case (0, _) | (_, None) =>
            for {
              passive    <- env.passiveView.toList
              sentNodes  = msg.originalSender :: (msg.activeNodes ++ msg.passiveNodes)
              replyNodes <- env.selectN(passive, env.cfg.shuffleNActive + env.cfg.shuffleNPassive)
              _          <- env.addAllToPassiveView(sentNodes)
            } yield Tagged
              .write[InitialProtocol[T]](InitialProtocol.ShuffleReply(replyNodes, sentNodes))
              .flatMap(m => ZIO.accessM[Transport[T]](_.transport.send(msg.originalSender, m)))
          case (_, Some(ttl)) =>
            for {
              active <- env.activeView.keys
              next   <- env.selectOne(active.filterNot(_ == msg.sender))
            } yield ZIO.foreach(next)(send[T, Protocol[T]](_, msg.copy(sender = env.myself, ttl = ttl)))
        }
        .commit
        .flatten
    }

}
