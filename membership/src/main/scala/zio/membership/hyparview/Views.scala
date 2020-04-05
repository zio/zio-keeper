package zio.membership.hyparview

import zio._
import zio.stm._
import zio.membership.SendError

private object Views {

  private[hyparview] trait Service[T] {
    def myself: T
    def activeViewCapacity: Int
    def passiveViewCapacity: Int

    def activeView: STM[Nothing, Set[T]]
    def passiveView: STM[Nothing, Set[T]]

    final def activeViewSize: STM[Nothing, Int] =
      activeView.map(_.size)

    final def passiveViewSize: STM[Nothing, Int] =
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

    final def viewState: STM[Nothing, ViewState] =
      for {
        activeViewSize  <- activeViewSize
        passiveViewSize <- passiveViewSize
      } yield ViewState(activeViewSize, activeViewCapacity, passiveViewSize, passiveViewCapacity)
  }

  def myself[T: Tagged]: ZSTM[Views[T], Nothing, T] =
    ZSTM.access(_.get.myself)

  def activeViewCapacity[T: Tagged]: ZSTM[Views[T], Nothing, Int] =
    ZSTM.access(_.get.activeViewCapacity)

  def passiveViewCapacity[T: Tagged]: ZSTM[Views[T], Nothing, Int] =
    ZSTM.access(_.get.passiveViewCapacity)

  def activeView[T: Tagged]: ZSTM[Views[T], Nothing, Set[T]] =
    ZSTM.accessM(_.get.activeView)

  def passiveView[T: Tagged]: ZSTM[Views[T], Nothing, Set[T]] =
    ZSTM.accessM(_.get.passiveView)

  def activeViewSize[T: Tagged]: ZSTM[Views[T], Nothing, Int] =
    ZSTM.accessM(_.get.activeViewSize)

  def passiveViewSize[T: Tagged]: ZSTM[Views[T], Nothing, Int] =
    ZSTM.accessM(_.get.passiveViewSize)

  def isActiveViewFull[T: Tagged]: ZSTM[Views[T], Nothing, Boolean] =
    ZSTM.accessM(_.get.isActiveViewFull)

  def isPassiveViewFull[T: Tagged]: ZSTM[Views[T], Nothing, Boolean] =
    ZSTM.accessM(_.get.isPassiveViewFull)

  def addToActiveView[T: Tagged](
    node: T,
    send: ActiveProtocol[T] => IO[SendError, Unit],
    disconnect: UIO[Unit]
  ): ZSTM[Views[T], Unit, Unit] =
    ZSTM.accessM(_.get.addToActiveView(node, send, disconnect))

  def addToPassiveView[T: Tagged](node: T): ZSTM[Views[T], Nothing, Unit] =
    ZSTM.accessM(_.get.addToPassiveView(node))

  def addAllToPassiveView[T: Tagged](nodes: List[T]): ZSTM[Views[T], Nothing, Unit] =
    ZSTM.accessM(_.get.addAllToPassiveView(nodes))

  def removeFromActiveView[T: Tagged](node: T): ZSTM[Views[T], Nothing, Unit] =
    ZSTM.accessM(_.get.removeFromActiveView(node))

  def removeFromPassiveView[T: Tagged](node: T): ZSTM[Views[T], Nothing, Unit] =
    ZSTM.accessM(_.get.removeFromPassiveView(node))

  def addShuffledNodes[T: Tagged](
    sentOriginally: Set[T],
    replied: Set[T]
  ): ZSTM[Views[T], Nothing, Unit] =
    ZSTM.accessM(_.get.addShuffledNodes(sentOriginally, replied))

  def viewState[T: Tagged]: ZSTM[Views[T], Nothing, ViewState] =
    ZSTM.accessM(_.get.viewState)

  def send[T: Tagged](to: T, msg: ActiveProtocol[T]): ZIO[Views[T], SendError, Unit] =
    ZIO.accessM(_.get.send(to, msg))

  def fromConfig[T: Tagged](
    localAddr: T
  ): ZLayer[HyParViewConfig with TRandom, Nothing, Views[T]] =
    ZLayer.fromEffect {
      for {
        hpvc    <- URIO.environment[HyParViewConfig]
        conf    <- hpvc.get.getConfig
        service <- makeViews(localAddr, conf.activeViewCapacity, conf.passiveViewCapacity)
      } yield service
    }

  def live[T: Tagged](
    myself: T,
    activeViewCapacity: Int,
    passiveViewCapacity: Int
  ): ZLayer[TRandom, Nothing, Views[T]] =
    ZLayer.fromEffect(makeViews(myself, activeViewCapacity, passiveViewCapacity))

  private def makeViews[T](
    myself0: T,
    activeViewCapacity0: Int,
    passiveViewCapacity0: Int
  ): URIO[TRandom, Service[T]] =
    for {
      activeView0  <- TMap.empty[T, (ActiveProtocol[T] => IO[SendError, Unit], UIO[Unit])].commit
      passiveView0 <- TSet.empty[T].commit
      tRandom      <- URIO.environment[TRandom].map(_.get)
    } yield new Service[T] {

      val myself: T =
        myself0

      val activeViewCapacity: Int =
        activeViewCapacity0

      val passiveViewCapacity: Int =
        passiveViewCapacity0

      val activeView: STM[Nothing, Set[T]] =
        activeView0.keys.map(_.toSet)

      val passiveView: STM[Nothing, Set[T]] =
        passiveView0.toList.map(_.toSet)

      val isActiveViewFull: STM[Nothing, Boolean] =
        activeViewSize.map(_ >= activeViewCapacity)

      val isPassiveViewFull: STM[Nothing, Boolean] =
        passiveViewSize.map(_ >= passiveViewCapacity)

      def send(to: T, msg: ActiveProtocol[T]): IO[SendError, Unit] =
        activeView0
          .get(to)
          .commit
          .get
          .foldM(
            _ => IO.fail(SendError.NotConnected),
            n =>
              n._1(msg)
                .foldM(
                  {
                    case e: SendError.TransportFailed => n._2 *> IO.fail(e)
                    case e                            => IO.fail(e)
                  },
                  _ => IO.unit
                )
          )

      def addToActiveView(
        node: T,
        send: ActiveProtocol[T] => IO[SendError, Unit],
        disconnect: UIO[Unit]
      ): STM[Unit, Unit] =
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

      def removeFromActiveView(node: T): STM[Nothing, Unit] =
        activeView0.delete(node)

      def addToPassiveView(node: T): STM[Nothing, Unit] =
        for {
          inActive  <- activeView0.contains(node)
          inPassive <- passiveView0.contains(node)
          _ <- if (node == myself || inActive || inPassive) STM.unit
              else {
                for {
                  size <- passiveViewSize
                  _    <- if (size < passiveViewCapacity) STM.unit else dropOneFromPassive
                  _    <- passiveView0.put(node)
                } yield ()
              }
        } yield ()

      def addAllToPassiveView(remaining: List[T]): STM[Nothing, Unit] =
        remaining match {
          case Nil     => STM.unit
          case x :: xs => addToPassiveView(x) *> addAllToPassiveView(xs)
        }

      def removeFromPassiveView(node: T): STM[Nothing, Unit] =
        passiveView0.delete(node)

      def addShuffledNodes(sentOriginally: Set[T], replied: Set[T]): STM[Nothing, Unit] =
        for {
          _         <- passiveView0.removeIf(sentOriginally.contains)
          size      <- passiveViewSize
          _         <- dropNFromPassive(replied.size - (passiveViewCapacity - size))
          _         <- addAllToPassiveView(replied.toList)
          remaining <- passiveViewSize.map(passiveViewCapacity - _)
          _         <- addAllToPassiveView(sentOriginally.take(remaining).toList)
        } yield ()

      private def dropNFromPassive(n: Int): STM[Nothing, Unit] =
        if (n <= 0) STM.unit else (dropOneFromPassive *> dropNFromPassive(n - 1))

      private val dropOneFromPassive: STM[Nothing, Unit] =
        for {
          list    <- passiveView0.toList
          dropped <- tRandom.selectOne(list)
          _       <- STM.foreach(dropped)(passiveView0.delete(_))
        } yield ()
    }
}
