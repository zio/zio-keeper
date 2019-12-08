package zio.membership.hyparview

import zio._
import zio.stm._
import zio.membership.transport.ChunkConnection
import zio.macros.delegate._
import com.github.ghik.silencer.silent
import zio.random.Random

private[hyparview] trait Env[T] {
  val env: Env.Service[T]
}

private[hyparview] object Env {

  final class Using[T] {

    def apply[R <: Env[T], E, A](f: Env.Service[T] => ZIO[R, E, A]): ZIO[R, E, A] =
      ZIO.environment[Env[T]].flatMap(r => f(r.env))
  }

  def get[T]: ZIO[Env[T], Nothing, Env.Service[T]] = ZIO.environment[Env[T]].map(_.env)
  def using[T]: Using[T]                           = new Using[T]

  def withEnv[T](localAddr: T, config: Config) = enrichWithM[Env[T]] {
    @silent("deprecated")
    val makePickRandom: ZIO[Random, Nothing, Int => STM[Nothing, Int]] =
      for {
        seed    <- random.nextInt
        sRandom = new scala.util.Random(seed)
        ref     <- TRef.make(Stream.continually((i: Int) => sRandom.nextInt(i))).commit
      } yield (i: Int) => ref.modify(s => (s.head(i), s.tail))
    for {
      activeView0  <- TMap.empty[T, ChunkConnection].commit
      passiveView0 <- TSet.empty[T].commit
      pickRandom0  <- makePickRandom
    } yield new Env[T] {
      val env = new Env.Service[T] {
        override val myself      = localAddr
        override val activeView  = activeView0
        override val passiveView = passiveView0
        override val pickRandom  = pickRandom0
        override val cfg         = config
      }
    }
  }

  trait Service[T] {
    val myself: T
    val activeView: TMap[T, ChunkConnection]
    val passiveView: TSet[T]
    val pickRandom: Int => STM[Nothing, Int]
    val cfg: Config

    lazy val isActiveViewFull: STM[Nothing, Boolean] =
      activeView.keys.map(_.size >= cfg.activeViewCapactiy)

    lazy val isPassiveViewFull: STM[Nothing, Boolean] =
      passiveView.size.map(_ >= cfg.passiveViewCapacity)

    lazy val dropOneActiveToPassive: STM[Nothing, Option[T]] =
      for {
        active  <- activeView.keys
        dropped <- selectOne(active)
        _       <- STM.foreach(dropped)(addNodeToPassiveView(_))
      } yield dropped

    def addNodeToActiveView(node: T, connection: ChunkConnection): STM[Nothing, Option[T]] =
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

    def addNodeToPassiveView(node: T): STM[Nothing, Unit] =
      for {
        inActive  <- activeView.contains(node)
        inPassive <- passiveView.contains(node)
        _ <- if (node == myself || inActive || inPassive) STM.unit
            else {
              for {
                size <- passiveView.size
                _ <- if (size < cfg.passiveViewCapacity) STM.unit
                    else {
                      for {
                        list    <- passiveView.toList
                        dropped <- selectOne(list)
                        _       <- STM.foreach(dropped)(passiveView.delete(_))
                      } yield ()
                    }
                _ <- passiveView.put(node)
              } yield ()
            }
      } yield ()

    lazy val promoteRandom: STM[Nothing, Option[T]] =
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

    def selectOne[A](values: List[A]): STM[Nothing, Option[A]] =
      if (values.isEmpty) STM.succeed(None)
      else {
        for {
          index    <- pickRandom(values.size)
          selected = values(index)
        } yield Some(selected)
      }

    def selectN[A](values: List[A], n: Int): STM[Nothing, List[A]] =
      if (values.isEmpty) STM.succeed(Nil)
      else {
        def go(remaining: List[A], toPick: Int, acc: List[A]): STM[Nothing, List[A]] =
          (remaining, toPick) match {
            case (Nil, _) | (_, 0) => STM.succeed(acc)
            case _ =>
              pickRandom(remaining.size).flatMap { index =>
                val x  = values(index)
                val xs = values.drop(index)
                go(xs, toPick - 1, x :: acc)
              }
          }
        go(values, n, Nil)
      }

    def addAllToPassiveView(remaining: List[T]): STM[Nothing, Unit] =
      remaining match {
        case Nil     => STM.unit
        case x :: xs => addNodeToPassiveView(x) *> addAllToPassiveView(xs)
      }

  }
}
