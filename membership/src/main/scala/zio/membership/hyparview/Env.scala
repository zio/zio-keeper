package zio.membership.hyparview

import zio._
import zio.stm._
import zio.membership.transport.ChunkConnection

private[hyparview] trait Env[T] {
  val env: Env.Service[T]
}

private[hyparview] object Env {
  def myself[T]                 = ZIO.environment[Env[T]].map(_.env.myself)
  def activeView[T]             = ZIO.environment[Env[T]].map(_.env.activeView)
  def passiveView[T]            = ZIO.environment[Env[T]].map(_.env.passiveView)
  def cfg[T]                    = ZIO.environment[Env[T]].map(_.env.cfg)
  def dropOneActiveToPassive[T] = ZIO.accessM[Env[T]](_.env.dropOneActiveToPassive.commit)

  def addNodeToActiveView[T](node: T, con: ChunkConnection) =
    ZIO.accessM[Env[T]](_.env.addNodeToActiveView(node, con).commit)
  def addNodeToPassiveView[T](node: T) = ZIO.accessM[Env[T]](_.env.addNodeToPassiveView(node).commit)
  def promoteRandom[T]                 = ZIO.accessM[Env[T]](_.env.promoteRandom.commit)

  final case class Config(
    activeViewCapactiy: Int,
    passiveViewCapacity: Int,
    arwl: Int,
    prwl: Int,
    shuffleNActive: Int,
    shuffleNPassive: Int,
    shuffleTTL: Int
  )

  trait Service[T] {
    val myself: T
    val activeView: TMap[T, ChunkConnection]
    val passiveView: TSet[T]
    val pickRandom: Int => STM[Nothing, Int]
    val cfg: Config

    val isActiveViewFull =
      activeView.keys.map(_.size >= cfg.activeViewCapactiy)

    val isPassiveViewFull =
      passiveView.size.map(_ >= cfg.passiveViewCapacity)

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
                        _    <- passiveView.delete(node)
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
        promoted <- if (activeFull) STM.succeed(None)
                   else {
                     for {
                       passive  <- passiveView.toList
                       promoted <- selectOne(passive)
                     } yield promoted
                   }
      } yield promoted

    def selectOne[A](values: List[A]) =
      if (values.isEmpty) STM.succeed(None)
      else {
        for {
          index    <- pickRandom(values.size - 1)
          selected = values(index)
        } yield Some(selected)
      }

    def selectN[A](values: List[A], n: Int) =
      if (values.isEmpty) STM.succeed(Nil)
      else {
        def go(remaining: List[A], toPick: Int, acc: List[A]): STM[Nothing, List[A]] =
          (remaining, toPick) match {
            case (Nil, _) | (_, 0) => STM.succeed(acc)
            case _ =>
              pickRandom(remaining.size - 1).flatMap { index =>
                val x  = values(index)
                val xs = values.drop(index)
                go(xs, toPick - 1, x :: acc)
              }
          }
        go(values, n, Nil)
      }

    def addAllToPassiveView(remaining: List[T]): STM[Nothing, Unit] = remaining match {
      case Nil     => STM.unit
      case x :: xs => addNodeToPassiveView(x) *> addAllToPassiveView(xs)
    }

    private[this] def dropOne[A](set: TSet[A]) =
      for {
        list    <- set.toList
        dropped <- selectOne(list)
        _       <- STM.foreach(dropped)(set.delete(_))
      } yield dropped

  }
}
