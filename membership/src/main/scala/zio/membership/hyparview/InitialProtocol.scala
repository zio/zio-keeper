package zio.membership.hyparview

import zio._
import zio.membership.transport.ChunkConnection
import zio._
import zio.membership.ByteCodec
import zio.membership.transport.Transport
import zio.membership.Error
import zio.stm._
import zio.console.Console
import upickle.default._

sealed private[hyparview] trait InitialProtocol[T]

private[hyparview] object InitialProtocol {

  implicit def tagged[T](
    implicit
    c1: ByteCodec[Neighbor[T]],
    c2: ByteCodec[Join[T]],
    c3: ByteCodec[ForwardJoinReply[T]],
    c4: ByteCodec[ShuffleReply[T]]
  ): Tagged[InitialProtocol[T]] =
    Tagged.instance(
      {
        case _: Neighbor[T]         => 0
        case _: Join[T]             => 1
        case _: ForwardJoinReply[T] => 2
        case _: ShuffleReply[T]     => 3
      }, {
        case 0 => c1.asInstanceOf[ByteCodec[InitialProtocol[T]]]
        case 1 => c2.asInstanceOf[ByteCodec[InitialProtocol[T]]]
        case 2 => c3.asInstanceOf[ByteCodec[InitialProtocol[T]]]
        case 3 => c4.asInstanceOf[ByteCodec[InitialProtocol[T]]]
      }
    )

  final case class Neighbor[T](
    sender: T,
    isHighPriority: Boolean
  ) extends InitialProtocol[T]

  object Neighbor {

    implicit def codec[T: ReadWriter]: ByteCodec[Neighbor[T]] =
      ByteCodec.fromReadWriter(macroRW[Neighbor[T]])
  }

  final case class Join[T](
    sender: T
  ) extends InitialProtocol[T]

  object Join {

    implicit def codec[T: ReadWriter]: ByteCodec[Join[T]] =
      ByteCodec.fromReadWriter(macroRW[Join[T]])
  }

  final case class ForwardJoinReply[T](
    sender: T
  ) extends InitialProtocol[T]

  object ForwardJoinReply {

    implicit def codec[T: ReadWriter]: ByteCodec[ForwardJoinReply[T]] =
      ByteCodec.fromReadWriter(macroRW[ForwardJoinReply[T]])
  }

  final case class ShuffleReply[T](
    passiveNodes: List[T],
    sentOriginally: List[T]
  ) extends InitialProtocol[T]

  object ShuffleReply {

    implicit def codec[T: ReadWriter]: ByteCodec[ShuffleReply[T]] =
      ByteCodec.fromReadWriter(macroRW[ShuffleReply[T]])
  }

  def handleInitialProtocol[T](
    con: ChunkConnection
  )(
    implicit
    ev1: Tagged[InitialProtocol[T]],
    ev2: Tagged[Protocol[T]]
  ): ZIO[Console with Env[T] with Transport[T], Error, Unit] =
    con.receive
      .take(1)
      .foreach { msg =>
        Tagged.read[InitialProtocol[T]](msg).flatMap {
          case m: InitialProtocol.Neighbor[T]         => handleNeighbor(m, con)
          case m: InitialProtocol.Join[T]             => handleJoin(m, con)
          case m: InitialProtocol.ForwardJoinReply[T] => handleForwardJoinReply(m, con)
          case m: InitialProtocol.ShuffleReply[T]     => handleShuffleReply(m)
        }
      }
      .ensuring(con.close)

  // individual message handlers --------------------------------------------------------------------

  def handleNeighbor[T](
    msg: InitialProtocol.Neighbor[T],
    con: ChunkConnection
  )(
    implicit
    ev1: Tagged[Protocol[T]],
    ev2: Tagged[InitialProtocol[T]]
  ) =
    ZIO.environment[Env[T]].map(_.env).flatMap { env =>
      val accept = for {
        reply <- Tagged.write[NeighborProtocol](NeighborProtocol.Accept)
        _     <- con.send(reply)
        _     <- addConnection(msg.sender, con)
      } yield ()

      val reject = for {
        reply <- Tagged.write[NeighborProtocol](NeighborProtocol.Reject)
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

  def handleJoin[T](
    msg: InitialProtocol.Join[T],
    con: ChunkConnection
  )(
    implicit
    ev1: Tagged[Protocol[T]],
    ev2: Tagged[InitialProtocol[T]]
  ) =
    ZIO.environment[Env[T]].map(_.env).flatMap { env =>
      for {
        others <- env.activeView.keys.map(_.filterNot(_ == msg.sender)).commit
        _ <- ZIO.foreachPar_(others)(
              node =>
                send[T, Protocol[T]](node, Protocol.ForwardJoin(env.myself, msg.sender, TimeToLive(env.cfg.arwl))).ignore
            )
        _ <- addConnection(msg.sender, con)
      } yield ()
    }

  def handleForwardJoinReply[T](
    msg: InitialProtocol.ForwardJoinReply[T],
    con: ChunkConnection
  )(
    implicit
    ev1: Tagged[Protocol[T]],
    ev2: Tagged[InitialProtocol[T]]
  ) = addConnection(msg.sender, con)

  def handleShuffleReply[T](
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
