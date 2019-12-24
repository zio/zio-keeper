package zio.membership.hyparview

import zio._
import zio.stm._
import zio.stream._
import com.github.ghik.silencer.silent
import zio.random.Random
import scala.collection.immutable.{ Stream => ScStream }
import zio.macros.delegate._

private[hyparview] trait Views[T] {
  val views: Views.Service[Any, T]
}

private[hyparview] object Views {

  private[hyparview] trait Service[-R, T] {
    def myself: T
    def activeViewCapacity: Int
    def passiveViewCapacity: Int

    def activeView: STM[Nothing, Set[T]]
    def passiveView: STM[Nothing, Set[T]]

    def activeViewSize: STM[Nothing, Int] =
      activeView.map(_.size)

    def passiveViewSize: STM[Nothing, Int] =
      passiveView.map(_.size)

    def isActiveViewFull: STM[Nothing, Boolean]

    def isPassiveViewFull: STM[Nothing, Boolean]

    def send(to: T, msg: ActiveProtocol[T]): IO[Unit, Unit]

    def addNodeToActiveView(
      node: T,
      send: ActiveProtocol[T] => IO[Unit, Unit],
      disconnect: UIO[Unit],
      registerExitHook: (Boolean => UIO[Unit]) => UIO[Unit]
    ): UIO[Unit]

    def addNodeToPassiveView(node: T): STM[Nothing, Unit]
    def addAllToPassiveView(remaining: List[T]): STM[Nothing, Unit]

    def selectOne[A](values: List[A]): STM[Nothing, Option[A]]
    def selectN[A](values: List[A], n: Int): STM[Nothing, List[A]]

    def removeFromPassiveView(node: T): STM[Nothing, Unit]

    def addShuffledNodes(
      sentOriginally: Set[T],
      replied: Set[T]
    ): STM[Nothing, Unit]
  }

  final class Using[T] {

    def apply[R <: Views[T], E, A](f: Service[Any, T] => ZIO[R, E, A]): ZIO[R, E, A] =
      ZIO.environment[Views[T]].flatMap(r => f(r.views))
  }

  final class UsingManaged[T] {

    def apply[R <: Views[T], E, A](f: Service[Any, T] => ZManaged[R, E, A]): ZManaged[R, E, A] =
      ZManaged.environment[Views[T]].flatMap(r => f(r.views))
  }

  def using[T]: Using[T]               = new Using[T]
  def usingManaged[T]: UsingManaged[T] = new UsingManaged[T]

  def withViews[T](
    myself: T,
    activeViewCapacity: Int,
    passiveViewCapacity: Int
  ) = enrichWithM[Views[T]](make(myself, activeViewCapacity, passiveViewCapacity))

  def make[T](
    myself0: T,
    activeViewCapacity0: Int,
    passiveViewCapacity0: Int
  ): ZIO[Random, Nothing, Views[T]] = {
    @silent("deprecated")
    val makePickRandom: ZIO[Random, Nothing, Int => STM[Nothing, Int]] =
      for {
        seed    <- random.nextInt
        sRandom = new scala.util.Random(seed)
        ref     <- TRef.make(ScStream.continually((i: Int) => sRandom.nextInt(i))).commit
      } yield (i: Int) => ref.modify(s => (s.head(i), s.tail))
    for {
      toAdd <- Queue.bounded[
                (T, ActiveProtocol[T] => IO[Unit, Unit], (Boolean => UIO[Unit]) => UIO[Unit], UIO[Unit], UIO[Unit])
              ](256)
      activeView0  <- TMap.empty[T, (ActiveProtocol[T] => IO[Unit, Unit], UIO[Unit])].commit
      passiveView0 <- TSet.empty[T].commit
      pickRandom   <- makePickRandom
      _ <- Stream
            .fromQueue(toAdd)
            .foreach {
              case (addr, sendMsg, registerExitHook, disconnect, done) =>
                for {
                  toDrop <- activeView0
                             .get(addr)
                             .flatMap {
                               case None =>
                                 activeView0.values.map(aV => (aV.size >= activeViewCapacity0, aV)).flatMap {
                                   case (true, nodes) =>
                                     def selectOne[A](values: List[A]): STM[Nothing, Option[A]] =
                                       if (values.isEmpty) STM.succeed(None)
                                       else {
                                         for {
                                           index    <- pickRandom(values.size)
                                           selected = values(index)
                                         } yield Some(selected)
                                       }
                                     selectOne(nodes)
                                   case (false, _) => STM.succeed(None)
                                 }
                               case Some(old) => STM.succeed(Some(old))
                             }
                             .commit
                  _ <- ZIO.foreach(toDrop)(_._2)
                  _ <- ZIO.uninterruptible {
                        for {
                          _ <- (for {
                                hasSpace <- activeView0.keys.map(_.size <= activeViewCapacity0)
                                _        <- if (hasSpace) activeView0.put(addr, (sendMsg, disconnect)) else STM.retry
                              } yield ()).commit
                          _ <- registerExitHook {
                                case true  => (activeView0.delete(addr) *> passiveView0.put(addr)).commit
                                case false => activeView0.delete(addr).commit
                              }
                        } yield ()
                      }
                  _ <- done
                } yield ()
            }
            .fork
    } yield new Views[T] {
      val views = new Service[Any, T] {

        override val myself =
          myself0

        override val activeViewCapacity =
          activeViewCapacity0

        override val passiveViewCapacity =
          passiveViewCapacity0

        override val activeView =
          activeView0.keys.map(_.toSet)

        override val passiveView =
          passiveView0.toList.map(_.toSet)

        override val isActiveViewFull =
          activeViewSize.map(_ >= activeViewCapacity)

        override val isPassiveViewFull =
          passiveViewSize.map(_ >= passiveViewCapacity)

        override def send(to: T, msg: ActiveProtocol[T]) =
          activeView0
            .get(to)
            .commit
            .get
            .foldM(
              _ => ZIO.fail(()),
              n =>
                n._1(msg)
                  .foldM(
                    _ => n._2 *> IO.fail(()),
                    _ => IO.unit
                  )
            )

        override def addNodeToActiveView(
          node: T,
          send: ActiveProtocol[T] => IO[Unit, Unit],
          disconnect: UIO[Unit],
          registerExitHook: (Boolean => UIO[Unit]) => UIO[Unit]
        ) =
          if (node == myself) ZIO.unit
          else {
            for {
              done <- Promise.make[Nothing, Unit]
              _    <- toAdd.offer((node, send, registerExitHook, disconnect, done.succeed(()).unit))
              _    <- done.await
            } yield ()
          }

        override def addNodeToPassiveView(node: T): STM[Nothing, Unit] =
          for {
            inActive  <- activeView0.contains(node)
            inPassive <- passiveView0.contains(node)
            _ <- if (node == myself || inActive || inPassive) STM.unit
                else {
                  for {
                    size <- passiveViewSize
                    _ <- if (size < passiveViewCapacity) STM.unit
                        else {
                          for {
                            list    <- passiveView0.toList
                            dropped <- selectOne(list)
                            _       <- STM.foreach(dropped)(passiveView0.delete(_))
                          } yield ()
                        }
                    _ <- passiveView0.put(node)
                  } yield ()
                }
          } yield ()

        override def addAllToPassiveView(remaining: List[T]): STM[Nothing, Unit] =
          remaining match {
            case Nil     => STM.unit
            case x :: xs => addNodeToPassiveView(x) *> addAllToPassiveView(xs)
          }

        override def selectOne[A](values: List[A]): STM[Nothing, Option[A]] =
          if (values.isEmpty) STM.succeed(None)
          else {
            for {
              index    <- pickRandom(values.size)
              selected = values(index)
            } yield Some(selected)
          }

        override def selectN[A](values: List[A], n: Int): STM[Nothing, List[A]] =
          if (values.isEmpty) STM.succeed(Nil)
          else {
            def go(remaining: Vector[A], toPick: Int, acc: Vector[A]): STM[Nothing, Vector[A]] =
              (remaining, toPick) match {
                case (Vector(), _) | (_, 0) => STM.succeed(acc)
                case _ =>
                  pickRandom(remaining.size).flatMap { index =>
                    val x  = remaining(index)
                    val xs = remaining.patch(index, Nil, 1)
                    go(xs, toPick - 1, x +: acc)
                  }
              }
            go(values.toVector, n, Vector()).map(_.toList)
          }

        override def removeFromPassiveView(node: T) =
          passiveView0.delete(node)

        override def addShuffledNodes(sentOriginally: Set[T], replied: Set[T]) =
          for {
            _         <- passiveView0.removeIf(sentOriginally.contains)
            _         <- addAllToPassiveView(replied.toList)
            remaining <- passiveViewSize.map(passiveViewCapacity - _)
            _         <- addAllToPassiveView(sentOriginally.take(remaining).toList)
          } yield ()
      }
    }
  }
}
