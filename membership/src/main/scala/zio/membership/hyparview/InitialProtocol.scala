package zio.membership.hyparview

import zio._
import zio.membership.transport.ChunkConnection
import zio._
import zio.membership.ByteCodec
import zio.membership.ByteCodec._
import zio.membership.transport.Transport
import zio.membership.Error
import zio.stm._
import zio.console.Console

sealed trait InitialProtocol[T]

object InitialProtocol {
  def decodeChunk[T](chunk: Chunk[Byte]): UIO[InitialProtocol[T]] = ???

  final case class Neighbor[T](
    sender: T,
    isHighPriority: Boolean
  ) extends InitialProtocol[T]

  final case class Join[T](
    sender: T
  ) extends InitialProtocol[T]

  final case class ForwardJoinReply[T](
    sender: T
  ) extends InitialProtocol[T]

  final case class ShuffleReply[T](
    passiveNodes: List[T],
    sentOriginally: List[T]
  ) extends InitialProtocol[T]

  private[hyparview] def initialProtocolHandler[T](
    con: ChunkConnection
  )(
    implicit
    ev1: ByteCodec[Protocol.Disconnect[T]],
    ev2: ByteCodec[Protocol.ForwardJoin[T]],
    ev3: ByteCodec[Protocol.Shuffle[T]],
    ev4: ByteCodec[InitialProtocol.ForwardJoinReply[T]],
    ev5: ByteCodec[InitialProtocol.ShuffleReply[T]]
  ): ZIO[Console with Env[T] with Transport[T], Error, Unit] =
    con
      .receive
      .take(1)
      .foreach { msg =>
        InitialProtocol.decodeChunk[T](msg).flatMap {
          case m: InitialProtocol.Neighbor[T] => handleNeighbor(m, con)
          case m: InitialProtocol.Join[T] => handleJoin(m, con)
          case m: InitialProtocol.ForwardJoinReply[T] => handleForwardJoinReply(m, con)
          case m: InitialProtocol.ShuffleReply[T] => handleShuffleReply(m)
        }
      }
      .ensuring(con.close)

  private[hyparview] def handleNeighbor[T](
    msg: InitialProtocol.Neighbor[T],
    con: ChunkConnection
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
        reply <- encode(NeighborProtocol.Accept)
        _     <- con.send(reply)
        _     <- addConnection(msg.sender, con)
      } yield ()

      val reject = for {
        reply <- encode(NeighborProtocol.Reject)
        _     <- con.send(reply)
      } yield ()

      if (msg.isHighPriority) {
        accept
      } else {
        (for {
          full <- env.isActiveViewFull
          task <- if (full) {
                   env
                     .addNodeToPassiveView(msg.sender)
                     .as(
                       reject
                     )
                 } else {
                   STM.succeed(accept)
                 }
        } yield task).commit.flatten
      }
    }

  private[hyparview] def handleJoin[T](
    msg: InitialProtocol.Join[T],
    con: ChunkConnection
  )(
    implicit
    ev1: ByteCodec[Protocol.Disconnect[T]],
    ev2: ByteCodec[Protocol.ForwardJoin[T]],
    ev3: ByteCodec[Protocol.Shuffle[T]],
    ev4: ByteCodec[InitialProtocol.ForwardJoinReply[T]],
    ev5: ByteCodec[InitialProtocol.ShuffleReply[T]]
  ) =
    ZIO.environment[Env[T]].map(_.env).flatMap { env =>
      for {
        others  <- env.activeView.keys.map(_.filterNot(_ == msg.sender)).commit
        _ <- ZIO.foreachPar_(others)(
              node => send(node, Protocol.ForwardJoin(env.myself, msg.sender, TimeToLive(env.cfg.arwl))).ignore
            )
        _ <- addConnection(msg.sender, con)
      } yield ()
    }

  private[hyparview] def handleForwardJoinReply[T](
    msg: InitialProtocol.ForwardJoinReply[T],
    con: ChunkConnection
  )( implicit
    ev1: ByteCodec[Protocol.Disconnect[T]],
    ev2: ByteCodec[Protocol.ForwardJoin[T]],
    ev3: ByteCodec[Protocol.Shuffle[T]],
    ev4: ByteCodec[InitialProtocol.ForwardJoinReply[T]],
    ev5: ByteCodec[InitialProtocol.ShuffleReply[T]]
  ) = addConnection(msg.sender, con)

  private[hyparview] def handleShuffleReply[T](
    msg: InitialProtocol.ShuffleReply[T]
  ) =
    ZIO.environment[Env[T]].map(_.env).flatMap { env =>
      val sentOriginally = msg.sentOriginally.toSet
      (for {
        _         <- env.passiveView.removeIf(sentOriginally.contains)
        _         <- env.addAllToPassiveView(msg.passiveNodes)
        remaining <- env.passiveView.size.map(env.cfg.passiveViewCapacity - _)
        _         <- env.addAllToPassiveView(sentOriginally.take(remaining).toList)
      } yield ()).commit
    }



}
