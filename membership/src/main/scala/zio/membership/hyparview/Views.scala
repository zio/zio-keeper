package zio.membership.hyparview

import zio._
import zio.stm._
import zio.macros.delegate._
import zio.membership.SendError

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

    def send(to: T, msg: ActiveProtocol[T]): IO[SendError, Unit]

    def addToActiveView(
      node: T,
      send: ActiveProtocol[T] => IO[SendError, Unit],
      disconnect: UIO[Unit]
    ): STM[Unit, Unit]

    def addToPassiveView(node: T): STM[Nothing, Unit]
    def addAllToPassiveView(nodes: List[T]): STM[Nothing, Unit]

    def removeFromActiveView(node: T): STM[Nothing, Unit]
    def removeFromPassiveView(node: T): STM[Nothing, Unit]

    def addShuffledNodes(
      sentOriginally: Set[T],
      replied: Set[T]
    ): STM[Nothing, Unit]

    def viewState: STM[Nothing, ViewState] =
      for {
        activeViewSize  <- activeViewSize
        passiveViewSize <- passiveViewSize
      } yield ViewState(activeViewSize, activeViewCapacity, passiveViewSize, passiveViewCapacity)
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
  ): ZIO[TRandom, Nothing, Views[T]] = {
    for {
      tRandom      <- ZIO.environment[TRandom].map(_.tRandom)
      activeView0  <- TMap.empty[T, (ActiveProtocol[T] => IO[SendError, Unit], UIO[Unit])].commit
      passiveView0 <- TSet.empty[T].commit
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
              _ => ZIO.fail(SendError.NotConnected),
              n =>
                n._1(msg)
                  .foldM(
                    {
                      case e: SendError.TransportFailed => n._2 *> ZIO.fail(e)
                      case e                            => ZIO.fail(e)
                    },
                    _ => IO.unit
                  )
            )

        override def addToActiveView(
          node: T,
          send: ActiveProtocol[T] => IO[SendError, Unit],
          disconnect: UIO[Unit]
        ) =
          if (node == myself) STM.unit
          else {
            val abort = for {
              inActive   <- activeView0.contains(node)
              activeFull <- isActiveViewFull
            } yield inActive || activeFull
            abort.flatMap {
              case false =>
                for {
                  _ <- activeView0.put(node, (send, disconnect))
                  _ <- passiveView0.delete(node)
                } yield ()
              case true =>
                STM.fail(())
            }
          }

        override def removeFromActiveView(node: T): STM[Nothing, Unit] =
          activeView0.delete(node)

        override def addToPassiveView(node: T): STM[Nothing, Unit] =
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
                            dropped <- tRandom.selectOne(list)
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
            case x :: xs => addToPassiveView(x) *> addAllToPassiveView(xs)
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
