package zio.membership.hyparview

import zio._
import zio.membership.transport.ChunkConnection
import zio._
import zio.membership.ByteCodec
import zio.membership.ByteCodec._
import zio.membership.transport.Transport
import zio.stm._
import zio.console.Console
import zio.console.putStrLn

sealed trait Protocol[T]

object Protocol {
  def decodeChunk[T](chunk: Chunk[Byte]): UIO[Protocol[T]] = ???

  final case class Disconnect[T](
    sender: T,
    alive: Boolean
  ) extends Protocol[T]

  final case class ForwardJoin[T](
    sender: T,
    originalSender: T,
    ttl: TimeToLive
  ) extends Protocol[T]

  final case class Shuffle[T](
    sender: T,
    originalSender: T,
    activeNodes: List[T],
    passiveNodes: List[T],
    ttl: TimeToLive
  ) extends Protocol[T]

  private[hyparview] def protocolHandler[T](
    con: ChunkConnection
  )(
    implicit
    ev1: ByteCodec[Protocol.Disconnect[T]],
    ev2: ByteCodec[Protocol.ForwardJoin[T]],
    ev3: ByteCodec[Protocol.Shuffle[T]],
    ev4: ByteCodec[InitialProtocol.ForwardJoinReply[T]],
    ev5: ByteCodec[InitialProtocol.ShuffleReply[T]]
  ): ZIO[Console with Env[T] with Transport[T], Nothing, Handler] =
    con
      .receive
      .foreach { msg =>
        Protocol.decodeChunk[T](msg).flatMap {
          case m: Protocol.Disconnect[T]     => handleDisconnect(m) *> con.close
          case m: Protocol.ForwardJoin[T]    => handleForwardJoin(m)
          case m: Protocol.Shuffle[T]        => handleShuffle(m)
        }
      }
      .onError(e => putStrLn(s"Handler failed with ${e}") *> con.close)
      .fork

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
    ev1: ByteCodec[Protocol.Disconnect[T]],
    ev2: ByteCodec[Protocol.ForwardJoin[T]],
    ev3: ByteCodec[Protocol.Shuffle[T]],
    ev4: ByteCodec[InitialProtocol.ForwardJoinReply[T]],
    ev5: ByteCodec[InitialProtocol.ShuffleReply[T]]
  ) =
    ZIO.environment[Env[T]].map(_.env).flatMap { env =>
      val accept = for {
        con <- ZIO.accessM[Transport[T]](_.transport.connect(msg.sender))
        reply <- encode(InitialProtocol.ForwardJoinReply(env.myself))
        _   <- con.send(reply)
        _   <- addConnection(msg.sender, con)
      } yield ()

      val process = env.activeView.keys.map(ks => (ks.size, msg.ttl.step)).flatMap {
        case (i, _) if i <= 1 =>
          STM.succeed(accept)
        case (_, None) =>
          STM.succeed(accept)
        case (_, Some(ttl)) =>
          for {
            list <- env.activeView.keys.map(_.filterNot(_ == msg.sender))
            next <- env.selectOne(list)
            _ <- if (ttl.count == env.cfg.prwl) env.addNodeToPassiveView(msg.sender) else STM.unit
          } yield ZIO.foreach(next)(send(_, msg.copy(sender = env.myself, ttl = ttl)))
      }
      process.commit.flatten
    }

    private[hyparview] def handleShuffle[T](
      msg: Protocol.Shuffle[T]
    )(
      implicit
      ev1: ByteCodec[Protocol.Shuffle[T]],
      ev2: ByteCodec[InitialProtocol.ShuffleReply[T]]
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
              } yield encode(InitialProtocol.ShuffleReply(replyNodes, sentNodes)).flatMap(m =>
                ZIO.accessM[Transport[T]](_.transport.send(msg.originalSender, m))
              )
            case (_, Some(ttl)) =>
              for {
                active <- env.activeView.keys
                next   <- env.selectOne(active.filterNot(_ == msg.sender))
              } yield ZIO.foreach(next)(send(_, msg.copy(sender = env.myself, ttl = ttl)))
          }
          .commit
          .flatten
      }

}
