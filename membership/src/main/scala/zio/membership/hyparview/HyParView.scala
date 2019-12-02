package zio.membership.hyparview

import zio.membership.transport.ChunkConnection
import zio.membership.Membership
import zio.membership.Error
import zio._
import zio.membership.ByteCodec
import zio.membership.ByteCodec._
import zio.membership.transport.Transport
import zio.stm._
import zio.random.Random

object HyParView {

  def apply[T](
    localAddr: T,
    activeViewCapactiy: Int,
    passiveViewCapacity: Int,
    arwl: Int,
    prwl: Int
  ): ZManaged[Transport[T] with Random, Nothing, Membership[T]] = ???

  final case class Config(
    activeViewCapactiy: Int,
    passiveViewCapacity: Int,
    arwl: Int,
    prwl: Int
  )

  //=========
  // internal
  //=========

  private[hyparview] type Handler[-R[_], M[_], T] = M[T] => ZIO[R[T], Nothing, Unit]

  private[hyparview] def send[T, M <: Protocol[T]: ByteCodec](to: T, msg: M): ZIO[Env[T] with Transport[T], Error, Unit] =
    for {
      chunk <- encode(msg)
      con   <- Env.activeView[T].flatMap(_.get(to).commit)
      _     <- ZIO.foreach(con)(_.send(chunk))
    } yield ()

  private[hyparview] def sendOneOf[T, M <: Protocol[T]: ByteCodec](to: T, msg: M): ZIO[Transport[T], Error, Unit] =
    for {
      chunk <- encode(msg)
      _ <- ZIO.accessM[Transport[T]](_.transport.send(to, chunk))
    } yield ()

  trait Env[T] {
    val env: Env.Service[T]
  }

  object Env {
    def myself[T] = ZIO.environment[Env[T]].map(_.env.myself)
    def activeView[T] = ZIO.environment[Env[T]].map(_.env.activeView)
    def passiveView[T] = ZIO.environment[Env[T]].map(_.env.passiveView)
    def cfg[T] = ZIO.environment[Env[T]].map(_.env.cfg)
    def dropOneActiveToPassive[T] = ZIO.accessM[Env[T]](_.env.dropOneActiveToPassive.commit)
    def addNodeToActiveView[T](node: T, connection: ChunkConnection) = ZIO.accessM[Env[T]](_.env.addNodeToActiveView(node, connection).commit)
    def addNodeToPassiveView[T](node: T) = ZIO.accessM[Env[T]](_.env.addNodeToPassiveView(node).commit)
    def promoteRandom[T] = ZIO.accessM[Env[T]](_.env.promoteRandom.commit)

    trait Service[T] {
      val myself: T
      val activeView: TMap[T, ChunkConnection]
      val passiveView: TSet[T]
      val pickRandom: Int => STM[Nothing, Int]
      val cfg: Config

      val dropOneActiveToPassive =
        for {
          active  <- activeView.keys
          dropped <- selectOne(active)
          _       <- STM.foreach(dropped)(addNodeToPassiveView(_))
        } yield dropped

      def addNodeToActiveView(node: T, connection: ChunkConnection) =
        if (node == myself) STM.succeed(None)
        else {
          for {
            contained <- activeView.contains(node)
            dropped <- if (contained) STM.succeed(None)
                      else {
                        for {
                          size <- activeView.keys.map(_.size)
                          dropped <- if (size >= cfg.activeViewCapactiy) dropOneActiveToPassive
                                      else STM.succeed(None)
                          _ <- activeView.put(node, connection)
                        } yield dropped
                      }
          } yield dropped
        }

      def addNodeToPassiveView(node: T) =
        for {
          inActive  <- activeView.contains(node)
          inPassive <- passiveView.contains(node)
        } yield {
          if (node == myself || inActive || inPassive) STM.unit
          else {
            for {
              size <- passiveView.size
              _    <- if (size >= cfg.passiveViewCapacity) dropOne(passiveView) else STM.unit
              _    <- passiveView.put(node)
            } yield ()
          }
        }

      val promoteRandom =
        for {
          activeFull <- activeView.keys.map(_.size >= cfg.activeViewCapactiy)
          promoted <- if (activeFull) STM.succeed(None) else {
            for {
              passive <- passiveView.toList
              promoted <- selectOne(passive)
              _        <- STM.foreach(promoted)(passiveView.delete(_))
            } yield promoted
          }
        } yield promoted

      private[this] def selectOne[A](values: List[A]) =
        if (values.isEmpty) STM.succeed(None) else {
          for {
            index    <- pickRandom(values.size)
            selected = values(index)
          } yield Some(selected)
        }

      private[this] def dropOne[A](set: TSet[A]) =
        for {
          list    <- set.toList
          dropped <- selectOne(list)
          _       <- STM.foreach(dropped)(set.delete(_))
        } yield dropped

    }
  }
}
