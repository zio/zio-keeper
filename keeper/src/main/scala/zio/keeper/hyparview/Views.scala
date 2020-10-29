package zio.keeper.hyparview

import zio.keeper.NodeAddress
import zio.stm._
import zio.stream.{ Stream, ZStream }
import zio._
import zio.keeper.hyparview.ViewEvent._
import zio.keeper.hyparview.Message.{ Neighbor, PeerMessage, ViewMessage }
import java.time.Instant
import java.time.Duration
import zio.clock.Clock

object Views {

  trait Service {
    def activeView: USTM[Set[NodeAddress]]
    def activeViewCapacity: USTM[Int]
    def activeViewSize: USTM[Int]
    def addAllToPassiveView(nodes: List[NodeAddress]): USTM[Unit]
    def addShuffledNodes(sentOriginally: Set[NodeAddress], replied: Set[NodeAddress]): USTM[Unit]
    def addToActiveView(node: NodeAddress, send: ViewMessage => USTM[_], disconnect: Boolean => USTM[_]): USTM[Unit]
    def addToPassiveView(node: NodeAddress): USTM[Unit]
    def doNeighbor: USTM[Unit]
    def events: Stream[Nothing, ViewEvent]
    def myself: USTM[NodeAddress]
    def passiveView: USTM[Set[NodeAddress]]
    def passiveViewCapacity: USTM[Int]
    def passiveViewSize: USTM[Int]
    def peerMessage(node: NodeAddress, msg: PeerMessage): USTM[Unit]
    def removeFromActiveView(node: NodeAddress, keepInPassive: Boolean): USTM[Unit]
    def removeFromPassiveView(node: NodeAddress): USTM[Unit]
    def send(to: NodeAddress, msg: ViewMessage): USTM[Unit]
  }

  def activeView: ZSTM[Views, Nothing, Set[NodeAddress]] =
    ZSTM.accessM(_.get.activeView)

  def activeViewCapacity: ZSTM[Views, Nothing, Int] =
    ZSTM.accessM(_.get.activeViewCapacity)

  def activeViewSize: ZSTM[Views, Nothing, Int] =
    ZSTM.accessM(_.get.activeViewSize)

  def addAllToPassiveView(nodes: List[NodeAddress]): ZSTM[Views, Nothing, Unit] =
    ZSTM.accessM(_.get.addAllToPassiveView(nodes))

  def addShuffledNodes(sentOriginally: Set[NodeAddress], replied: Set[NodeAddress]): ZSTM[Views, Nothing, Unit] =
    ZSTM.accessM(_.get.addShuffledNodes(sentOriginally, replied))

  def addToActiveView(
    node: NodeAddress,
    send: ViewMessage => USTM[_],
    disconnect: Boolean => USTM[_]
  ): ZSTM[Views, Nothing, Unit] =
    ZSTM.accessM(_.get.addToActiveView(node, send, disconnect))

  def addToPassiveView(node: NodeAddress): ZSTM[Views, Nothing, Unit] =
    ZSTM.accessM(_.get.addToPassiveView(node))

  def doNeighbor: ZSTM[Views, Nothing, Unit] =
    ZSTM.accessM(_.get.doNeighbor)

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
    ZSTM.accessM(_.get.passiveViewSize)

  def peerMessage(node: NodeAddress, msg: PeerMessage): ZSTM[Views, Nothing, Unit] =
    ZSTM.accessM(_.get.peerMessage(node, msg))

  def removeFromActiveView(node: NodeAddress, keepInPassive: Boolean): ZSTM[Views, Nothing, Unit] =
    ZSTM.accessM(_.get.removeFromActiveView(node, keepInPassive))

  def removeFromPassiveView(node: NodeAddress): ZSTM[Views, Nothing, Unit] =
    ZSTM.accessM(_.get.removeFromPassiveView(node))

  def send(to: NodeAddress, msg: ViewMessage): ZSTM[Views, Nothing, Unit] =
    ZSTM.accessM(_.get.send(to, msg))

  def viewState: ZSTM[Views, Nothing, ViewState] =
    for {
      activeViewSize      <- activeViewSize
      activeViewCapacity  <- activeViewCapacity
      passiveViewSize     <- passiveViewSize
      passiveViewCapacity <- passiveViewCapacity
    } yield ViewState(activeViewSize, activeViewCapacity, passiveViewSize, passiveViewCapacity)

  def live: ZLayer[TRandom with HyParViewConfig with Clock, Nothing, Views] = {
    def startNeighborHandler(
      config: HyParViewConfig.Config,
      queue: TQueue[Unit],
      views: Service
    ) =
      for {
        ref <- Ref.makeManaged(Map.empty[NodeAddress, Instant])
        _ <- Stream
              .fromTQueue(queue)
              .foreachManaged { _ =>
                for {
                  lastNeighbor <- ref.get
                  now          <- clock.instant
                  nextOpt <- ZSTM.atomically {
                              for {
                                activeView  <- views.activeView
                                passiveView <- views.passiveView
                                candidates = passiveView
                                  .filter(
                                    lastNeighbor
                                      .get(_)
                                      .fold(true)(
                                        Duration.between(_, now).compareTo(config.neighborBackoff) > 0
                                      )
                                  )
                                  .toList
                                nextOpt <- TRandom.selectOne(candidates)
                                _ <- nextOpt.fold(STM.unit) { next =>
                                      views.send(next, Neighbor(config.address, activeView.isEmpty))
                                    }
                              } yield nextOpt
                            }
                  _ <- nextOpt.fold(ZIO.unit)(n => ref.update(_ + (n -> now)))
                } yield ()
              }
              .fork
      } yield ()

    def makeService(
      config: HyParViewConfig.Config,
      neighborQueue: TQueue[Unit]
    ) =
      for {
        _                <- ZIO.dieMessage("active view capacity must be greater than 0").when(config.activeViewCapacity <= 0)
        _                <- ZIO.dieMessage("passive view capacity must be greater than 0").when(config.passiveViewCapacity <= 0)
        viewEvents       <- TQueue.bounded[ViewEvent](config.viewsEventBuffer).commit
        activeViewState  <- TMap.empty[NodeAddress, (ViewMessage => USTM[Any], Boolean => USTM[Any])].commit
        passiveViewState <- TSet.empty[NodeAddress].commit
        tRandom          <- URIO.environment[TRandom].map(_.get)
      } yield new Service {

        override val activeView: USTM[Set[NodeAddress]] =
          activeViewState.keys.map(_.toSet)

        override val activeViewCapacity: USTM[Int] =
          STM.succeedNow(config.activeViewCapacity)

        override def activeViewSize: USTM[Int] =
          activeViewState.size

        override def addAllToPassiveView(remaining: List[NodeAddress]): USTM[Unit] =
          remaining match {
            case Nil     => STM.unit
            case x :: xs => addToPassiveView(x) *> addAllToPassiveView(xs)
          }

        override def addShuffledNodes(sentOriginally: Set[NodeAddress], replied: Set[NodeAddress]): USTM[Unit] =
          for {
            _         <- STM.foreach(sentOriginally.toList)(removeFromPassiveView)
            size      <- passiveViewSize
            capacity  <- passiveViewCapacity
            _         <- dropNFromPassive(replied.size - (capacity - size))
            _         <- addAllToPassiveView(replied.toList)
            remaining <- passiveViewSize.map(capacity - _)
            _         <- addAllToPassiveView(sentOriginally.take(remaining).toList)
          } yield ()

        override def addToActiveView(
          node: NodeAddress,
          send: ViewMessage => USTM[_],
          disconnect: Boolean => USTM[_]
        ): USTM[Unit] =
          STM.unless(node == config.address) {
            ZSTM.ifM(activeViewState.contains(node))(
              {
                for {
                  _ <- removeFromActiveView(node, false)
                  _ <- addToActiveView(node, send, disconnect)
                } yield ()
              }, {
                for {
                  _ <- {
                    for {
                      activeView <- activeView
                      node       <- tRandom.selectOne(activeView.toList)
                      _          <- node.fold[USTM[Unit]](STM.unit)(removeFromActiveView(_, true))
                    } yield ()
                  }.whenM(activeViewState.size.map(_ >= config.activeViewCapacity))
                  _ <- viewEvents.offer(AddedToActiveView(node))
                  _ <- activeViewState.put(node, (send, disconnect))
                } yield ()
              }
            )
          }

        override def addToPassiveView(node: NodeAddress): USTM[Unit] =
          for {
            inActive            <- activeViewState.contains(node)
            inPassive           <- passiveViewState.contains(node)
            passiveViewCapacity <- passiveViewCapacity
            _ <- if (node == config.address || inActive || inPassive) STM.unit
                else {
                  for {
                    size <- passiveViewState.size
                    _    <- dropOneFromPassive.when(size >= passiveViewCapacity)
                    _    <- viewEvents.offer(AddedToPassiveView(node))
                    _    <- passiveViewState.put(node)
                  } yield ()
                }
          } yield ()

        override def doNeighbor: USTM[Unit] =
          neighborQueue.offer(())

        override val events: Stream[Nothing, ViewEvent] =
          Stream.fromTQueue(viewEvents)

        override val myself: USTM[NodeAddress] =
          STM.succeed(config.address)

        override val passiveView: USTM[Set[NodeAddress]] =
          passiveViewState.toList.map(_.toSet)

        override val passiveViewCapacity: USTM[Int] =
          STM.succeed(config.passiveViewCapacity)

        override def passiveViewSize: USTM[Int] =
          passiveViewState.size

        override def peerMessage(node: NodeAddress, msg: PeerMessage): USTM[Unit] =
          viewEvents.offer(ViewEvent.PeerMessageReceived(node, msg))

        override def removeFromActiveView(node: NodeAddress, keepInPassive: Boolean): USTM[Unit] =
          activeViewState
            .get(node)
            .get
            .foldM(
              _ => STM.unit, {
                case (_, disconnect) =>
                  for {
                    _ <- viewEvents.offer(RemovedFromActiveView(node))
                    _ <- activeViewState.delete(node)
                    _ <- addToPassiveView(node).when(keepInPassive)
                    _ <- disconnect(keepInPassive)
                    _ <- doNeighbor
                  } yield ()
              }
            )

        override def removeFromPassiveView(node: NodeAddress): USTM[Unit] = {
          viewEvents.offer(RemovedFromPassiveView(node)) *> passiveViewState.delete(node)
        }.whenM(passiveViewState.contains(node))

        override def send(to: NodeAddress, msg: ViewMessage): USTM[Unit] =
          activeViewState
            .get(to)
            .get
            .foldM(
              _ => viewEvents.offer(UnhandledMessage(to, msg)),
              _._1(msg).unit
            )

        private def dropNFromPassive(n: Int): USTM[Unit] =
          (dropOneFromPassive *> dropNFromPassive(n - 1)).when(n > 0)

        private val dropOneFromPassive: USTM[Unit] =
          for {
            list    <- passiveViewState.toList
            dropped <- tRandom.selectOne(list)
            _       <- STM.foreach_(dropped)(removeFromPassiveView(_))
          } yield ()
      }

    ZLayer.fromManaged(
      for {
        config  <- HyParViewConfig.getConfig.toManaged_
        queue   <- TQueue.unbounded[Unit].commit.toManaged_
        service <- makeService(config, queue).toManaged_
        _       <- startNeighborHandler(config, queue, service)
      } yield service
    )
  }

}
