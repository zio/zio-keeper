package zio.keeper.hyparview

import zio.keeper.{ NodeAddress, SendError }
import zio.stm.{ STM, TMap, TSet, ZSTM }
import zio._

object Views {

  trait Service {
    def myself: STM[Nothing, NodeAddress]
    def activeViewCapacity: STM[Nothing, Int]
    def passiveViewCapacity: STM[Nothing, Int]

    def activeView: STM[Nothing, Set[NodeAddress]]
    def passiveView: STM[Nothing, Set[NodeAddress]]

    final def activeViewSize: STM[Nothing, Int] =
      activeView.map(_.size)

    final def passiveViewSize: STM[Nothing, Int] =
      passiveView.map(_.size)

    def isActiveViewFull: STM[Nothing, Boolean]

    def isPassiveViewFull: STM[Nothing, Boolean]

    def send(to: NodeAddress, msg: ActiveProtocol): IO[SendError, Unit]

    def addToActiveView(
      node: NodeAddress,
      send: ActiveProtocol => IO[SendError, Unit],
      disconnect: UIO[Unit]
    ): STM[Unit, Unit]

    def addToPassiveView(node: NodeAddress): STM[Nothing, Unit]
    def addAllToPassiveView(nodes: List[NodeAddress]): STM[Nothing, Unit]

    def removeFromActiveView(node: NodeAddress): STM[Nothing, Unit]
    def removeFromPassiveView(node: NodeAddress): STM[Nothing, Unit]

    def addShuffledNodes(
      sentOriginally: Set[NodeAddress],
      replied: Set[NodeAddress]
    ): STM[Nothing, Unit]

    final def viewState: STM[Nothing, ViewState] =
      for {
        activeViewSize      <- activeViewSize
        activeViewCapacity  <- activeViewCapacity
        passiveViewSize     <- passiveViewSize
        passiveViewCapacity <- passiveViewCapacity
      } yield ViewState(activeViewSize, activeViewCapacity, passiveViewSize, passiveViewCapacity)
  }

  def myself: ZSTM[Views, Nothing, NodeAddress] =
    ZSTM.accessM(_.get.myself)

  def activeViewCapacity: ZSTM[Views, Nothing, Int] =
    ZSTM.accessM(_.get.activeViewCapacity)

  def passiveViewCapacity: ZSTM[Views, Nothing, Int] =
    ZSTM.accessM(_.get.passiveViewCapacity)

  def activeView: ZSTM[Views, Nothing, Set[NodeAddress]] =
    ZSTM.accessM(_.get.activeView)

  def passiveView: ZSTM[Views, Nothing, Set[NodeAddress]] =
    ZSTM.accessM(_.get.passiveView)

  def activeViewSize: ZSTM[Views, Nothing, Int] =
    ZSTM.accessM(_.get.activeViewSize)

  def passiveViewSize: ZSTM[Views, Nothing, Int] =
    ZSTM.accessM(_.get.passiveViewSize)

  def isActiveViewFull: ZSTM[Views, Nothing, Boolean] =
    ZSTM.accessM(_.get.isActiveViewFull)

  def isPassiveViewFull: ZSTM[Views, Nothing, Boolean] =
    ZSTM.accessM(_.get.isPassiveViewFull)

  def addToActiveView(
    node: NodeAddress,
    send: ActiveProtocol => IO[SendError, Unit],
    disconnect: UIO[Unit]
  ): ZSTM[Views, Unit, Unit] =
    ZSTM.accessM(_.get.addToActiveView(node, send, disconnect))

  def addToPassiveView(node: NodeAddress): ZSTM[Views, Nothing, Unit] =
    ZSTM.accessM(_.get.addToPassiveView(node))

  def addAllToPassiveView(nodes: List[NodeAddress]): ZSTM[Views, Nothing, Unit] =
    ZSTM.accessM(_.get.addAllToPassiveView(nodes))

  def removeFromActiveView(node: NodeAddress): ZSTM[Views, Nothing, Unit] =
    ZSTM.accessM(_.get.removeFromActiveView(node))

  def removeFromPassiveView(node: NodeAddress): ZSTM[Views, Nothing, Unit] =
    ZSTM.accessM(_.get.removeFromPassiveView(node))

  def addShuffledNodes(
    sentOriginally: Set[NodeAddress],
    replied: Set[NodeAddress]
  ): ZSTM[Views, Nothing, Unit] =
    ZSTM.accessM(_.get.addShuffledNodes(sentOriginally, replied))

  def viewState: ZSTM[Views, Nothing, ViewState] =
    ZSTM.accessM(_.get.viewState)

  def send(to: NodeAddress, msg: ActiveProtocol): ZIO[Views, SendError, Unit] =
    ZIO.accessM(_.get.send(to, msg))

  def fromConfig(
    localAddr: NodeAddress
  ): ZLayer[HyParViewConfig with TRandom, Nothing, Views] =
    ZLayer.fromEffect {
      for {
        hpvc    <- URIO.environment[HyParViewConfig]
        conf    <- hpvc.get.getConfig
        service <- makeViews(localAddr, conf.activeViewCapacity, conf.passiveViewCapacity)
      } yield service
    }

  def live(
    myself: NodeAddress,
    activeViewCapacity: Int,
    passiveViewCapacity: Int
  ): ZLayer[TRandom, Nothing, Views] =
    ZLayer.fromEffect(makeViews(myself, activeViewCapacity, passiveViewCapacity))

  private def makeViews(
    myself0: NodeAddress,
    activeViewCapacity0: Int,
    passiveViewCapacity0: Int
  ): URIO[TRandom, Service] =
    for {
      activeView0  <- TMap.empty[NodeAddress, (ActiveProtocol => IO[SendError, Unit], UIO[Unit])].commit
      passiveView0 <- TSet.empty[NodeAddress].commit
      tRandom      <- URIO.environment[TRandom].map(_.get)
    } yield new Service {

      val myself: STM[Nothing, NodeAddress] =
        STM.succeed(myself0)

      val activeViewCapacity: STM[Nothing, Int] =
        STM.succeed(activeViewCapacity0)

      val passiveViewCapacity: STM[Nothing, Int] =
        STM.succeed(passiveViewCapacity0)

      val activeView: STM[Nothing, Set[NodeAddress]] =
        activeView0.keys.map(_.toSet)

      val passiveView: STM[Nothing, Set[NodeAddress]] =
        passiveView0.toList.map(_.toSet)

      val isActiveViewFull: STM[Nothing, Boolean] =
        activeViewSize.zipWith(activeViewCapacity)(_ >= _)

      val isPassiveViewFull: STM[Nothing, Boolean] =
        passiveViewSize.zipWith(passiveViewCapacity)(_ >= _)

      def send(to: NodeAddress, msg: ActiveProtocol): IO[SendError, Unit] =
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
        node: NodeAddress,
        send: ActiveProtocol => IO[SendError, Unit],
        disconnect: UIO[Unit]
      ): STM[Unit, Unit] =
        if (node == myself0) STM.unit
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

      def removeFromActiveView(node: NodeAddress): STM[Nothing, Unit] =
        activeView0.delete(node)

      def addToPassiveView(node: NodeAddress): STM[Nothing, Unit] =
        for {
          inActive            <- activeView0.contains(node)
          inPassive           <- passiveView0.contains(node)
          passiveViewCapacity <- passiveViewCapacity
          _ <- if (node == myself0 || inActive || inPassive) STM.unit
              else {
                for {
                  size <- passiveViewSize
                  _    <- if (size < passiveViewCapacity) STM.unit else dropOneFromPassive
                  _    <- passiveView0.put(node)
                } yield ()
              }
        } yield ()

      def addAllToPassiveView(remaining: List[NodeAddress]): STM[Nothing, Unit] =
        remaining match {
          case Nil     => STM.unit
          case x :: xs => addToPassiveView(x) *> addAllToPassiveView(xs)
        }

      def removeFromPassiveView(node: NodeAddress): STM[Nothing, Unit] =
        passiveView0.delete(node)

      def addShuffledNodes(sentOriginally: Set[NodeAddress], replied: Set[NodeAddress]): STM[Nothing, Unit] =
        for {
          _                   <- passiveView0.removeIf(sentOriginally.contains)
          size                <- passiveViewSize
          passiveViewCapacity <- passiveViewCapacity
          _                   <- dropNFromPassive(replied.size - (passiveViewCapacity - size))
          _                   <- addAllToPassiveView(replied.toList)
          remaining           <- passiveViewSize.map(passiveViewCapacity - _)
          _                   <- addAllToPassiveView(sentOriginally.take(remaining).toList)
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
