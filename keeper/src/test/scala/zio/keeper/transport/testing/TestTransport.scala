package zio.keeper.transport.testing

import zio._
import zio.stream._
import zio.stream.Stream.Pull
import zio.keeper.transport.{ ChunkConnection, Transport }
import zio.keeper.{ NodeAddress, TransportError }

object TestTransport {

  type Ip = Chunk[Byte]

  trait Service {
    def awaitAvailable(node: NodeAddress): UIO[Unit]
    def servers: UIO[Set[NodeAddress]]
    def existingConnections: UIO[List[(Ip, Ip)]]
    def asNode[R <: Has[_], E, A](ip: Ip)(zio: ZIO[R with Transport, E, A]): ZIO[R, E, A]
    def setConnectivity(f: (Ip, Ip) => Boolean): UIO[Unit]
  }

  def awaitAvailable(node: NodeAddress): URIO[TestTransport, Unit] =
    ZIO.accessM(_.get.awaitAvailable(node))

  def servers: URIO[TestTransport, Set[NodeAddress]] =
    ZIO.accessM(_.get.servers)

  def existingConnections: URIO[TestTransport, List[(Ip, Ip)]] =
    ZIO.accessM(_.get.existingConnections)

  def asNode[R <: TestTransport, E, A](ip: Ip)(zio: ZIO[R with Transport, E, A]): ZIO[R, E, A] =
    ZIO.accessM(_.get.asNode[R, E, A](ip)(zio))

  def setConnectivity(f: (Ip, Ip) => Boolean): URIO[TestTransport, Unit] =
    ZIO.accessM(_.get.setConnectivity(f))

  def transportLayer(ip: Ip): ZLayer[TestTransport, Nothing, Transport] =
    ZLayer.fromEffect(
      ZIO.accessM(_.get.asNode[TestTransport, Nothing, Transport.Service](ip)(ZIO.service[Transport.Service]))
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
          def servers: UIO[Set[NodeAddress]] =
            ref.get.map(_.nodes.keySet)
          def existingConnections: UIO[List[(Ip, Ip)]] =
            ref.get.map(_.connections.map { case (t1, t2, _) => (t1, t2) })
          def setConnectivity(f: (Ip, Ip) => Boolean): UIO[Unit] =
            ref
              .update { old =>
                val (remaining, disconnected) = old.connections.partition {
                  case (t1, t2, _) => (t1 == t2) || (f(t1, t2) && f(t2, t1))
                }
                val newState = old.copy(connections = remaining, f = f)
                ZIO
                  .foreach_(disconnected) {
                    _._3(TransportError.ExceptionWrapper(new RuntimeException("disconnected (setConnectivity)")))
                  }
                  .as(newState)
              }
          def asNode[R <: Has[_], E, A](ip: Ip)(zio: ZIO[R with Transport, E, A]): ZIO[R, E, A] = {
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
                  _ <- ZStream.fromEffect(
                        ZIO
                          .fail(TransportError.ExceptionWrapper(new RuntimeException("Cannot bind to this ip.")))
                          .when(addr.ip != ip)
                      )
                  connections <- ZStream.managed(
                                  Queue
                                    .unbounded[Managed[Nothing, ChunkConnection]]
                                    .toManaged(_.shutdown)
                                )
                  finalizers <- ZStream.managed {
                                 Ref.make[List[UIO[Unit]]](Nil).toManaged(_.get.flatMap(ZIO.collectAll(_)))
                               }
                  connect = (remote: Ip) =>
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
                                connections = (addr.ip, remote, (e: TransportError) => close(Some(e)).unit) :: old.connections
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
                            old.nodes
                              .get(addr)
                              .fold[IO[TransportError, State]] {
                                ZIO.succeedNow {
                                  old.copy(
                                    nodes = old.nodes + (addr -> ((remote: Ip) => connect(remote)))
                                  )
                                }
                              } { _ =>
                                ZIO.fail(
                                  TransportError.ExceptionWrapper(new RuntimeException("Address already in use."))
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
                    s.nodes.get(to).map((s.f(ip, to.ip), _)) match {
                      case None =>
                        ZIO.fail(TransportError.ExceptionWrapper(new RuntimeException("Node not available")))
                      case Some((false, _)) =>
                        ZIO.fail(TransportError.ExceptionWrapper(new RuntimeException("Can't reach node")))
                      case Some((true, request)) =>
                        request(ip).foldM(
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

  private type ConnectionRequest = Ip => IO[Unit, (ChunkConnection, UIO[Unit])]

  final private case class State(
    connections: List[(Ip, Ip, TransportError => UIO[Unit])],
    nodes: Map[NodeAddress, ConnectionRequest],
    f: (Ip, Ip) => Boolean,
    waiters: Map[NodeAddress, List[UIO[Unit]]]
  )

  private object State {

    def initial: State =
      State(Nil, Map.empty, (_, _) => true, Map.empty.withDefaultValue(Nil))
  }

}
