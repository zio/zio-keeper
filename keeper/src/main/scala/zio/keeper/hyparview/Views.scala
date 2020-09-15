package zio.keeper.hyparview

import zio.keeper.NodeAddress
import zio.stm._
import zio.stream.{ Stream, ZStream }
import zio._
import zio.keeper.hyparview.ViewEvent._

object Views {

  trait Service {
    def activeView: USTM[Set[NodeAddress]]
    def activeViewCapacity: USTM[Int]
    def addAllToPassiveView(nodes: List[NodeAddress]): USTM[Unit]
    def addToActiveView(node: NodeAddress, send: Message => USTM[_], disconnect: USTM[_]): USTM[Unit]
    def addToPassiveView(node: NodeAddress): USTM[Unit]
    def events: Stream[Nothing, ViewEvent]
    def myself: USTM[NodeAddress]
    def passiveView: USTM[Set[NodeAddress]]
    def passiveViewCapacity: USTM[Int]
    def removeFromActiveView(node: NodeAddress): USTM[Unit]
    def removeFromPassiveView(node: NodeAddress): USTM[Unit]
    def send(to: NodeAddress, msg: Message): USTM[Unit]
  }

  def activeView: ZSTM[Views, Nothing, Set[NodeAddress]] =
    ZSTM.accessM(_.get.activeView)

  def activeViewCapacity: ZSTM[Views, Nothing, Int] =
    ZSTM.accessM(_.get.activeViewCapacity)

  def activeViewSize: ZSTM[Views, Nothing, Int] =
    ZSTM.accessM(_.get.activeView.map(_.size))

  def addAllToPassiveView(nodes: List[NodeAddress]): ZSTM[Views, Nothing, Unit] =
    ZSTM.accessM(_.get.addAllToPassiveView(nodes))

  def addToActiveView(node: NodeAddress, send: Message => USTM[_], disconnect: USTM[_]): ZSTM[Views, Nothing, Unit] =
    ZSTM.accessM(_.get.addToActiveView(node, send, disconnect))

  def addToPassiveView(node: NodeAddress): ZSTM[Views, Nothing, Unit] =
    ZSTM.accessM(_.get.addToPassiveView(node))

  def events: ZStream[Views, Nothing, ViewEvent] =
    ZStream.accessStream(_.get.events)

  def isActiveViewFull: ZSTM[Views, Nothing, Boolean] =
    activeViewSize.zipWith(activeViewCapacity)(_ >= _)

  def isPassiveViewFull: ZSTM[Views, Nothing, Boolean] =
    passiveViewSize.zipWith(passiveViewCapacity)(_ >= _)

  def myself: ZSTM[Views, Nothing, NodeAddress] =
    ZSTM.accessM(_.get.myself)

  def passiveView: ZSTM[Views, Nothing, Set[NodeAddress]] =
    ZSTM.accessM(_.get.passiveView)

  def passiveViewCapacity: ZSTM[Views, Nothing, Int] =
    ZSTM.accessM(_.get.passiveViewCapacity)

  def passiveViewSize: ZSTM[Views, Nothing, Int] =
    ZSTM.accessM(_.get.passiveView.map(_.size))

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
    eventsBuffer: Int = 256
  ): ZLayer[TRandom, Nothing, Views] =
    ZLayer.fromEffect {
      for {
        _            <- ZIO.dieMessage("active view capacity must be greater than 0").when(activeViewCapacity0 <= 0)
        _            <- ZIO.dieMessage("passive view capacity must be greater than 0").when(passiveViewCapacity0 <= 0)
        viewEvents   <- TQueue.bounded[ViewEvent](eventsBuffer).commit
        activeView0  <- TMap.empty[NodeAddress, (Message => USTM[Any], USTM[Any])].commit
        passiveView0 <- TSet.empty[NodeAddress].commit
        tRandom      <- URIO.environment[TRandom].map(_.get)
      } yield new Service {

        override val activeView: USTM[Set[NodeAddress]] =
          activeView0.keys.map(_.toSet)

        override val activeViewCapacity: USTM[Int] =
          STM.succeedNow(activeViewCapacity0)

        override def addAllToPassiveView(remaining: List[NodeAddress]): USTM[Unit] =
          remaining match {
            case Nil     => STM.unit
            case x :: xs => addToPassiveView(x) *> addAllToPassiveView(xs)
          }

        override def addToActiveView(node: NodeAddress, send: Message => USTM[_], disconnect: USTM[_]): USTM[Unit] =
          STM.when(!(node == myself0)) {
            ZSTM.ifM(activeView0.contains(node))(
              {
                for {
                  _ <- removeFromActiveView(node)
                  _ <- viewEvents.offer(AddedToActiveView(node))
                  _ <- activeView0.put(node, (send, disconnect))
                } yield ()
              }, {
                for {
                  _ <- {
                    for {
                      activeView <- activeView
                      node       <- tRandom.selectOne(activeView.toList)
                      _          <- node.fold[USTM[Unit]](STM.unit)(removeFromActiveView)
                    } yield ()
                  }.whenM(activeView0.size.map(_ >= activeViewCapacity0))
                  _ <- viewEvents.offer(AddedToActiveView(node))
                  _ <- activeView0.put(node, (send, disconnect))
                } yield ()
              }
            )
          }

        override def addToPassiveView(node: NodeAddress): USTM[Unit] =
          for {
            inActive            <- activeView0.contains(node)
            inPassive           <- passiveView0.contains(node)
            passiveViewCapacity <- passiveViewCapacity
            _ <- if (node == myself0 || inActive || inPassive) STM.unit
                else {
                  for {
                    size <- passiveView0.size
                    _    <- dropOneFromPassive.when(size >= passiveViewCapacity)
                    _    <- viewEvents.offer(AddedToPassiveView(node))
                    _    <- passiveView0.put(node)
                  } yield ()
                }
          } yield ()

        override val events: Stream[Nothing, ViewEvent] =
          Stream.fromTQueue(viewEvents)

        override val myself: USTM[NodeAddress] =
          STM.succeed(myself0)

        override val passiveView: USTM[Set[NodeAddress]] =
          passiveView0.toList.map(_.toSet)

        override val passiveViewCapacity: USTM[Int] =
          STM.succeed(passiveViewCapacity0)

        override def removeFromActiveView(node: NodeAddress): USTM[Unit] =
          activeView0
            .get(node)
            .get
            .foldM(
              _ => STM.unit, {
                case (_, disconnect) =>
                  for {
                    _ <- viewEvents.offer(RemovedFromActiveView(node))
                    _ <- activeView0.delete(node)
                    _ <- disconnect
                  } yield ()
              }
            )

        override def removeFromPassiveView(node: NodeAddress): USTM[Unit] = {
          viewEvents.offer(RemovedFromPassiveView(node)) *> passiveView0.delete(node)
        }.whenM(passiveView0.contains(node))

        override def send(to: NodeAddress, msg: Message): USTM[Unit] =
          activeView0
            .get(to)
            .get
            .foldM(
              _ => viewEvents.offer(UnhandledMessage(to, msg)),
              _._1(msg).unit
            )

        private def dropNFromPassive(n: Int): USTM[Unit] =
          if (n <= 0) STM.unit else (dropOneFromPassive *> dropNFromPassive(n - 1))

        private val dropOneFromPassive: USTM[Unit] =
          for {
            list    <- passiveView0.toList
            dropped <- tRandom.selectOne(list)
            _       <- STM.foreach_(dropped)(removeFromPassiveView(_))
          } yield ()
      }
    }

}
