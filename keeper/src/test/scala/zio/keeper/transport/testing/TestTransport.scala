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

  def transportLayer(addr: NodeAddress): ZLayer[TestTransport, Nothing, Transport] =
    ZLayer.fromEffect(
      ZIO.accessM(_.get.asNode[TestTransport, Nothing, Transport.Service](addr)(ZIO.service[Transport.Service]))
    )

  val make: ZLayer[Any, Nothing, TestTransport] =
    ZLayer.fromEffect {
      RefM.make(State.initial).map { ref =>
        new Service {
          def awaitAvailable(node: NodeAddress): UIO[Unit] =
            Promise.make[Nothing, Unit].flatMap { available =>
              ref
                .modify[Any, Nothing, UIO[Unit]] { old =>
                  if (old.nodes.contains(node)) ZIO.succeedNow((ZIO.unit, old))
                  else {
                    val newState =
                      old.copy(waiters = old.waiters + (node -> (available.succeed(()).unit :: old.waiters(node))))
                    ZIO.succeedNow((available.await, newState))
                  }
                }
                .flatten
            }
          def nodes: UIO[Set[NodeAddress]] =
            ref.get.map(_.nodes.keySet)
          def existingConnections: UIO[Set[(NodeAddress, NodeAddress)]] =
            ref.get.map(_.connections.keySet)
          def setConnectivity(f: (NodeAddress, NodeAddress) => Boolean): UIO[Unit] =
            ref
              .update { old =>
                val (remaining, disconnected) = old.connections.toList.partition {
                  case ((t1, t2), _) => f(t1, t2) && f(t2, t1)
                }
                val newState = old.copy(connections = remaining.toMap, f = f)
                ZIO
                  .foreach_(disconnected) {
                    _._2(TransportError.ExceptionWrapper(new RuntimeException("disconnected (setConnectivity)")))
                  }
                  .as(newState)
              }
          def asNode[R <: Has[_], E, A](addr: NodeAddress)(zio: ZIO[R with Transport, E, A]): ZIO[R, E, A] = {
            val transport = new Transport.Service {
              def bind(addr: NodeAddress): Stream[TransportError, ChunkConnection] = {

                def completeWaiters(node: NodeAddress): UIO[Unit] =
                  ref
                    .update { old =>
                      val matching = old.waiters(node)
                      val newState = old.copy(waiters = old.waiters - node)
                      ZIO.collectAll(matching).as(newState)
                    }

                def makeConnection(
                  out: Chunk[Byte] => UIO[Unit],
                  in: UIO[Either[Option[TransportError], Chunk[Byte]]],
                  getSendStatus: UIO[Either[Option[TransportError], Unit]]
                ): UIO[ChunkConnection] =
                  for {
                    receiveEnd    <- Promise.make[Nothing, Option[TransportError]]
                    receiveStatus <- Ref.make[Either[Option[TransportError], Unit]](Right(()))
                  } yield new ChunkConnection {

                    override def send(data: Chunk[Byte]): IO[TransportError, Unit] =
                      getSendStatus.flatMap {
                        case Left(err) =>
                          err.fold[IO[TransportError, Nothing]](
                            ZIO.fail(TransportError.ExceptionWrapper(new RuntimeException("disconnected")))
                          )(ZIO.fail(_))
                        case _ =>
                          out(data)
                      }

                    override val receive: ZStream[Any, TransportError, Chunk[Byte]] =
                      ZStream {
                        ZManaged.succeed {
                          in.race(receiveEnd.await.map(Left(_))).flatMap {
                            _.fold(
                              e =>
                                receiveEnd.succeed(e) *> receiveStatus.set(Left(e)) *> e
                                  .fold[Pull[Any, TransportError, Nothing]](Pull.end)(Pull.fail(_)),
                              bytes =>
                                receiveStatus.get.flatMap {
                                  case Left(err) => err.fold[Pull[Any, TransportError, Nothing]](Pull.end)(Pull.fail(_))
                                  case _         => Pull.emit(Chunk.single(bytes))
                                }
                            )
                          }
                        }
                      }
                  }

                for {
                  connections <- ZStream.managed(
                                  Queue
                                    .unbounded[Managed[Nothing, ChunkConnection]]
                                    .toManaged(_.shutdown)
                                )
                  finalizers <- ZStream.managed {
                                 Ref.make[List[UIO[Unit]]](Nil).toManaged(_.get.flatMap(ZIO.collectAll(_)))
                               }
                  connect = (remote: NodeAddress) =>
                    for {
                      incoming      <- Queue.unbounded[Either[Option[TransportError], Chunk[Byte]]]
                      outgoing      <- Queue.unbounded[Either[Option[TransportError], Chunk[Byte]]]
                      sendStatusRef <- Ref.make[Either[Option[TransportError], Unit]](Right(()))
                      closeRef <- Ref.make { (c: Option[TransportError]) =>
                                   incoming.offer(Left(c)) *> outgoing.offer(Left(c)) *> sendStatusRef.set(Left(c))
                                 }
                      close = (c: Option[TransportError]) => closeRef.modify(old => (old, _ => ZIO.unit)).flatMap(_(c))
                      _ <- ref.update { old =>
                            ZIO.succeedNow {
                              old.copy(
                                connections = old.connections + ((addr, remote) -> (
                                  (e: TransportError) => close(Some(e)).unit
                                ))
                              )
                            }
                          }
                      _ <- finalizers.update(close(None) :: _)
                      con1 <- makeConnection(
                               msg => outgoing.offer(Right(msg)).unit,
                               incoming.take,
                               sendStatusRef.get
                             )
                      con2 <- makeConnection(
                               msg => incoming.offer(Right(msg)).unit,
                               outgoing.take,
                               sendStatusRef.get
                             )
                      _ <- connections.offer(ZManaged.make(ZIO.succeedNow(con1))(_ => close(None)))
                    } yield (con2, close(None))
                  _ <- ZStream.managed {
                        ZManaged.make {
                          ref.update { old =>
                            ZIO.succeedNow {
                              old.copy(
                                nodes = old.nodes + (addr -> ((remote: NodeAddress) => connect(remote)))
                              )
                            }
                          }
                        } { _ =>
                          ref.update(old => ZIO.succeedNow(old.copy(nodes = old.nodes - addr)))
                        }
                      }
                  _      <- ZStream.fromEffect(completeWaiters(addr))
                  result <- ZStream.fromQueue(connections).flatMap(ZStream.managed(_))
                } yield result
              }
              def connect(to: NodeAddress): Managed[TransportError, ChunkConnection] =
                ref
                  .mapM { s =>
                    s.nodes.get(to).map((s.f(addr, to), _)) match {
                      case None =>
                        ZIO.fail(TransportError.ExceptionWrapper(new RuntimeException("Node not available")))
                      case Some((false, _)) =>
                        ZIO.fail(TransportError.ExceptionWrapper(new RuntimeException("Can't reach node")))
                      case Some((true, request)) =>
                        request(addr).foldM(
                          _ => ZIO.fail(TransportError.ExceptionWrapper(new RuntimeException("Can't reach node"))), {
                            case (connection, close) =>
                              ZIO.succeedNow(ZManaged.make(ZIO.succeedNow(connection))(_ => close))
                          }
                        )
                    }
                  }
                  .get
                  .toManaged_
                  .flatten
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
