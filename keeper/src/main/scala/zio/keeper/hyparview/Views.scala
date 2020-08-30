package zio.keeper.hyparview

import zio.keeper.NodeAddress
import zio.stm._
import zio._

object Views {

  trait Service {
    def activeView: STM[Nothing, Set[NodeAddress]]
    def activeViewCapacity: STM[Nothing, Int]
    def addAllToPassiveView(nodes: List[NodeAddress]): STM[Nothing, Unit]
    def addToActiveView(
      node: NodeAddress,
      send: Message => STM[Nothing, Unit],
      disconnect: STM[Nothing, Unit]
    ): STM[Unit, Unit]
    def addToPassiveView(node: NodeAddress): STM[Nothing, Unit]
    def myself: STM[Nothing, NodeAddress]
    def passiveView: STM[Nothing, Set[NodeAddress]]
    def passiveViewCapacity: STM[Nothing, Int]
    def removeFromActiveView(node: NodeAddress): STM[Nothing, Unit]
    def removeFromPassiveView(node: NodeAddress): STM[Nothing, Unit]
    def send(to: NodeAddress, msg: Message): STM[Nothing, Unit]
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
    ZSTM.accessM(_.get.activeView.map(_.size))

  def passiveViewSize: ZSTM[Views, Nothing, Int] =
    ZSTM.accessM(_.get.passiveView.map(_.size))

  def isActiveViewFull: ZSTM[Views, Nothing, Boolean] =
    activeViewSize.zipWith(activeViewCapacity)(_ >= _)

  def isPassiveViewFull: ZSTM[Views, Nothing, Boolean] =
    passiveViewSize.zipWith(passiveViewCapacity)(_ >= _)

  def addToActiveView(
    node: NodeAddress,
    send: Message => STM[Nothing, Unit],
    disconnect: STM[Nothing, Unit]
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

  def send(to: NodeAddress, msg: Message): ZSTM[Views, Nothing, Unit] =
    ZSTM.accessM(_.get.send(to, msg))

  def viewState: ZSTM[Views, Nothing, ViewState] =
    for {
      activeViewSize      <- activeViewSize
      activeViewCapacity  <- activeViewCapacity
      passiveViewSize     <- passiveViewSize
      passiveViewCapacity <- passiveViewCapacity
    } yield ViewState(activeViewSize, activeViewCapacity, passiveViewSize, passiveViewCapacity)

  def live(
    myself0: NodeAddress,
    activeViewCapacity0: Int,
    passiveViewCapacity0: Int,
    newMessagesBuffer: Int = 256
  ): ZLayer[TRandom, Nothing, Views] =
    ZLayer.fromEffect {
      for {
        _            <- ZIO.dieMessage("active view capacity must be greater than 0").when(activeViewCapacity0 <= 0)
        _            <- ZIO.dieMessage("passive view capacity must be greater than 0").when(passiveViewCapacity0 <= 0)
        newMessages  <- TQueue.bounded[(NodeAddress, Message)](newMessagesBuffer).commit
        activeView0  <- TMap.empty[NodeAddress, (Message => STM[Nothing, Unit], STM[Nothing, Unit])].commit
        passiveView0 <- TSet.empty[NodeAddress].commit
        tRandom      <- URIO.environment[TRandom].map(_.get)
      } yield new Service {

        def addToActiveView(
          node: NodeAddress,
          send: Message => STM[Nothing, Unit],
          disconnect: STM[Nothing, Unit]
        ): STM[Unit, Unit] =
          if (node == myself0) STM.unit
          else {
            ZSTM.ifM(activeView0.contains(node))(STM.fail(()), {
              for {
                _ <- {
                  for {
                    activeView <- activeView
                    node       <- tRandom.selectOne(activeView.toList)
                    _          <- node.fold[STM[Unit, Unit]](STM.fail(())) { node =>
                      for {
                        _ <- activeView0.get(node).get.foldM(_ => STM.unit, _._2)
                        _ <- activeView0.delete(node)
                      } yield ()
                    }
                  } yield ()
                }.whenM(activeView0.size.map(_ >= activeViewCapacity0))
                _ <- activeView0.put(node, (send, disconnect))
              } yield ()
            })
          }

        def send(to: NodeAddress, msg: Message): STM[Nothing, Unit] =
          activeView0
            .get(to)
            .get
            .foldM(
              _ => newMessages.offer((to, msg)),
              { case (send, _) =>
                send(msg)
              }
            )

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
                    size <- passiveView0.size
                    _    <- dropOneFromPassive.when(size >= passiveViewCapacity)
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

        private def dropNFromPassive(n: Int): STM[Nothing, Unit] =
          if (n <= 0) STM.unit else (dropOneFromPassive *> dropNFromPassive(n - 1))

        private val dropOneFromPassive: STM[Nothing, Unit] =
          for {
            list    <- passiveView0.toList
            dropped <- tRandom.selectOne(list)
            _       <- STM.foreach_(dropped)(passiveView0.delete(_))
          } yield ()
      }
    }

}
