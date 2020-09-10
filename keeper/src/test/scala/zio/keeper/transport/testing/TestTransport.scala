package zio.keeper.transport.testing

import zio._
import zio.stream._
import zio.stream.Stream.Pull
import zio.keeper.transport.{ ChunkConnection, Transport }
import zio.keeper.{ NodeAddress, TransportError }

object TestTransport {

  trait Service {
    def awaitAvailable(node: NodeAddress): UIO[Unit]
    def nodes: UIO[Set[NodeAddress]]
    def existingConnections: UIO[Set[(NodeAddress, NodeAddress)]]
    def asNode[R <: Has[_], E, A](addr: NodeAddress)(zio: ZIO[R with Transport, E, A]): ZIO[R, E, A]
    def setConnectivity(f: (NodeAddress, NodeAddress) => Boolean): UIO[Unit]
  }

  def awaitAvailable(node: NodeAddress): URIO[TestTransport, Unit] =
    ZIO.accessM(_.get.awaitAvailable(node))

  def nodes: URIO[TestTransport, Set[NodeAddress]] =
    ZIO.accessM(_.get.nodes)

  def existingConnections: URIO[TestTransport, Set[(NodeAddress, NodeAddress)]] =
    ZIO.accessM(_.get.existingConnections)

  def asNode[R <: TestTransport, E, A](addr: NodeAddress)(zio: ZIO[R with Transport, E, A]): ZIO[R, E, A] =
    ZIO.accessM(_.get.asNode[R, E, A](addr)(zio))

  def setConnectivity(f: (NodeAddress, NodeAddress) => Boolean): URIO[TestTransport, Unit] =
    ZIO.accessM(_.get.setConnectivity(f))

  def make(
    messagesBuffer: Int = 128,
    connectionBuffer: Int = 12
  ): ZLayer[Any, Nothing, TestTransport] =
    ZLayer.fromEffect {
      Ref.make(State.initial).map { ref =>
        new Service {
          def awaitAvailable(node: NodeAddress): UIO[Unit] =
            Promise.make[Nothing, Unit].flatMap { available =>
              ref.modify { old =>
                if (old.nodes.contains(node)) (ZIO.unit, old)
                else
                  (
                    available.await,
                    (old.copy(waiters = old.waiters + (node -> (available.succeed(()).unit :: old.waiters(node)))))
                  )
              }.flatten
            }
          def nodes: UIO[Set[NodeAddress]] =
            ref.get.map(_.nodes.keySet)
          def existingConnections: UIO[Set[(NodeAddress, NodeAddress)]] =
            ref.get.map(_.connections.keySet)
          def setConnectivity(f: (NodeAddress, NodeAddress) => Boolean): UIO[Unit] =
            ref
              .modify { old =>
                val (remaining, disconnected) = old.connections.toList.partition {
                  case ((t1, t2), _) => f(t1, t2) && f(t2, t1)
                }
                (disconnected, old.copy(connections = remaining.toMap, f = f))
              }
              .flatMap(
                ZIO
                  .foreach_(_)(
                    _._2(TransportError.ExceptionWrapper(new RuntimeException("disconnected (setConnectivity)")))
                  )
              )
          def asNode[R <: Has[_], E, A](addr: NodeAddress)(zio: ZIO[R with Transport, E, A]): ZIO[R, E, A] = {
            val transport = new Transport.Service {
              def bind(addr: NodeAddress): Stream[TransportError, ChunkConnection] = {

                def completeWaiters(node: NodeAddress): UIO[Unit] =
                  ref
                    .modify { old =>
                      val matching = old.waiters(node)
                      (matching, old.copy(waiters = old.waiters - node))
                    }
                    .flatMap(ZIO.collectAll[Any, Nothing, Unit](_).unit)

                def makeConnection(
                  out: Chunk[Byte] => UIO[Unit],
                  in: UIO[Either[Option[TransportError], Chunk[Byte]]],
                  sendLock: Semaphore,
                  isConnected: UIO[Boolean]
                ): UIO[ChunkConnection] =
                  for {
                    receiveEnd <- Promise.make[Nothing, Option[TransportError]]
                  } yield new ChunkConnection {

                    override def send(data: Chunk[Byte]): IO[TransportError, Unit] =
                      sendLock.withPermit {
                        isConnected.flatMap {
                          case false =>
                            ZIO.fail(TransportError.ExceptionWrapper(new RuntimeException("connection failed")))
                          case true =>
                            out(data)
                        }
                      }

                    override val receive: ZStream[Any, TransportError, Chunk[Byte]] =
                      ZStream {
                        ZManaged.succeed {

                          val awaitDisconnect = receiveEnd.await.map(Left(_))

                          in.race(awaitDisconnect)
                            .flatMap(
                              _.fold(
                                e =>
                                  receiveEnd
                                    .succeed(e) *> e.fold[Pull[Any, TransportError, Nothing]](Pull.end)(Pull.fail(_)),
                                bytes => Pull.emit(Chunk.single(bytes))
                              )
                            )
                        }
                      }
                  }

                for {
                  connections <- ZStream.managed(
                                  Queue
                                    .bounded[ChunkConnection](connectionBuffer)
                                    .toManaged(_.shutdown)
                                )
                  connect <- ZStream.managed(
                              Ref
                                .make[ConnectionRequest] { remote =>
                                  for {
                                    incoming <- Queue
                                                 .bounded[Either[Option[TransportError], Chunk[Byte]]](messagesBuffer)
                                    outgoing <- Queue
                                                 .bounded[Either[Option[TransportError], Chunk[Byte]]](messagesBuffer)
                                    inSendLock  <- Semaphore.make(1)
                                    outSendLock <- Semaphore.make(1)
                                    connected   <- Ref.make(true)
                                    closeRef <- Ref.make(
                                                 (c: Option[TransportError]) =>
                                                   inSendLock.withPermit[Any, Nothing, Unit](
                                                     outSendLock.withPermit[Any, Nothing, Unit](
                                                       incoming.offer(Left(c)) *> outgoing.offer(Left(c)) *> connected
                                                         .set(false)
                                                     )
                                                   )
                                               )
                                    close = (c: Option[TransportError]) =>
                                      closeRef.modify(old => (old, _ => ZIO.unit)).flatMap(_(c))
                                    con1 <- makeConnection(
                                             msg => outgoing.offer(Right(msg)).unit,
                                             incoming.take,
                                             outSendLock,
                                             connected.get
                                           )
                                    _ <- connections.offer(con1)
                                    con2 <- makeConnection(
                                             msg => incoming.offer(Right(msg)).unit,
                                             outgoing.take,
                                             inSendLock,
                                             connected.get
                                           )
                                    _ <- ref.update(
                                          old =>
                                            old.copy(
                                              connections = old.connections + ((addr, remote) -> (
                                                (e: TransportError) => close(Some(e)).unit
                                              ))
                                            )
                                        )
                                  } yield (con2, close(None))
                                }
                                .toManaged(_.set(_ => ZIO.fail(())))
                            )
                  _ <- ZStream.managed(
                        ZManaged.make(
                          ref.update(
                            old =>
                              old.copy(
                                nodes = old.nodes + (addr -> ((remote: NodeAddress) => connect.get.flatMap(_(remote))))
                              )
                          )
                        )(_ => ref.update(old => old.copy(nodes = old.nodes - addr)))
                      )
                  _      <- ZStream.fromEffect(completeWaiters(addr))
                  result <- ZStream.fromQueue(connections)
                } yield result
              }
              def connect(to: NodeAddress): Managed[TransportError, ChunkConnection] =
                ref.get
                  .map(s => s.nodes.get(to).map((s.f(addr, to), _)))
                  .get
                  .toManaged_
                  .foldM(
                    _ => ZManaged.fail(TransportError.ExceptionWrapper(new RuntimeException("Node not available"))), {
                      case (false, _) =>
                        ZManaged.fail(TransportError.ExceptionWrapper(new RuntimeException("Can't reach node")))
                      case (true, request) =>
                        request(addr).toManaged_.foldM[Any, TransportError, ChunkConnection](
                          _ => ZManaged.fail(TransportError.ExceptionWrapper(new RuntimeException("Can't reach node"))),
                          { case (connection, close) => ZManaged.make(ZIO.succeedNow(connection))(_ => close) }
                        )
                    }
                  )
            }
            zio.provideSomeLayer[R](ZLayer.succeed(transport))
          }
        }
      }
    }

  private type ConnectionRequest = NodeAddress => IO[Unit, (ChunkConnection, UIO[Unit])]

  final private case class State(
    connections: Map[(NodeAddress, NodeAddress), TransportError => UIO[Unit]],
    nodes: Map[NodeAddress, ConnectionRequest],
    f: (NodeAddress, NodeAddress) => Boolean,
    waiters: Map[NodeAddress, List[UIO[Unit]]]
  )

  private object State {

    def initial: State =
      State(Map.empty, Map.empty, (_, _) => true, Map.empty.withDefaultValue(Nil))
  }

}
