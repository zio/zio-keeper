package zio.membership.transport.testing

import com.github.ghik.silencer.silent
import zio._
import zio.membership.TransportError
import zio.membership.transport.{ ChunkConnection, Transport }
import zio.stream.ZStream
import zio.stream.ZStream.Pull

object InMemoryTransport {

  final class AsNodePartiallyApplied[T](addr: T) {

    @silent
    def apply[R <: InMemoryTransport[T], E, A](zio: ZIO[R, E, A])(implicit ev: Tagged[T]): ZIO[R, E, A] =
      ZIO.accessM[R](_.get.asNode(addr)(zio))
  }

  final case class State[T](
    connections: List[(T, T, TransportError => UIO[Unit])],
    nodes: Map[T, ConnectionRequest[T]],
    f: (T, T) => Boolean,
    waiters: Map[T, List[UIO[Unit]]]
  )

  object State {

    def initial[T]: State[T] =
      State(Nil, Map.empty, (_, _) => true, Map.empty.withDefaultValue(Nil))
  }

  type ConnectionRequest[T] = T => IO[Unit, ChunkConnection]

  trait Service[T] {
    def awaitAvailable(node: T): UIO[Unit]
    def nodes: UIO[Set[T]]
    def existingConnections: UIO[Set[(T, T)]]
    def asNode[R1 <: InMemoryTransport[T], E, A](addr: T)(zio: ZIO[R1, E, A]): ZIO[R1, E, A]
    def setConnectivity(f: (T, T) => Boolean): UIO[Unit]
  }

  def awaitAvailable[T: Tagged](node: T): URIO[InMemoryTransport[T], Unit] =
    ZIO.accessM(_.get.awaitAvailable(node))

  def nodes[T: Tagged]: URIO[InMemoryTransport[T], Set[T]] =
    ZIO.accessM(_.get.nodes)

  def existingConnections[T: Tagged]: URIO[InMemoryTransport[T], Set[(T, T)]] =
    ZIO.accessM(_.get.existingConnections)

  def asNode[T](addr: T): AsNodePartiallyApplied[T] =
    new AsNodePartiallyApplied(addr)

  def setConnectivity[T: Tagged](f: (T, T) => Boolean): URIO[InMemoryTransport[T], Unit] =
    ZIO.accessM[InMemoryTransport[T]](_.get.setConnectivity(f))

  def make[T: Tagged](
    messagesBuffer: Int = 128,
    connectionBuffer: Int = 128
  ): ZLayer[Any, Nothing, Transport[T] with InMemoryTransport[T]] = ZLayer.fromEffectMany {
    for {
      identityRef <- FiberRef.make[Option[T]](None)
      ref         <- Ref.make(State.initial[T])
    } yield {

      def completeWaiters(node: T): UIO[Unit] =
        ref
          .modify { old =>
            val matching = old.waiters(node)
            (matching, old.copy(waiters = old.waiters - node))
          }
          .flatMap(ZIO.collectAll[Any, Nothing, Unit](_).unit)

      def checkIdentity(t: T): IO[TransportError, Unit] =
        identityRef.get.flatMap { addr =>
          if (addr.fold(false)(_ == t)) ZIO.unit
          else
            ZIO.fail(
              TransportError.ExceptionThrown(
                new RuntimeException("Invalid current identity. Did you enclose the invocation in `asNode`?")
              )
            )
        }

      val getIdentity: IO[TransportError, T] =
        identityRef.get.get.foldM(
          _ =>
            ZIO.fail(
              TransportError
                .ExceptionThrown(new RuntimeException("No identity set. Die you enclose the invocation in `asNode`?"))
            ),
          identity => ZIO.succeed(identity)
        )

      def makeConnection(
        out: Chunk[Byte] => UIO[Unit],
        in: UIO[Either[Option[TransportError], Chunk[Byte]]],
        sendLock: Semaphore,
        isConnected: UIO[Boolean],
        close0: Option[TransportError] => UIO[Unit]
      ): UIO[ChunkConnection] =
        for {
          receiveEnd <- Promise.make[Nothing, Option[TransportError]]
        } yield new ChunkConnection {

          override def send(data: Chunk[Byte]): ZIO[Any, TransportError, Unit] =
            sendLock.withPermit {
              isConnected.flatMap {
                case false =>
                  ZIO.fail(TransportError.ExceptionThrown(new RuntimeException("connection failed")))
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
                      e => receiveEnd.succeed(e) *> e.fold[Pull[Any, TransportError, Nothing]](Pull.end)(Pull.fail(_)),
                      Pull.emit(_)
                    )
                  )
              }
            }

          override val close: UIO[Unit] =
            close0(None)
        }

      Has.allOf[Transport.Service[T], Service[T]](
        new Transport.Service[T] {
          override def connect(to: T): ZManaged[Any, TransportError, ChunkConnection] =
            getIdentity
              .flatMap {
                id =>
                  ref.get
                    .map(s => s.nodes.get(to).map((s.f(id, to), _)))
                    .get
                    .foldM(
                      _ => ZIO.fail(TransportError.ExceptionThrown(new RuntimeException("Node not available"))), {
                        case (false, _) =>
                          ZIO.fail(TransportError.ExceptionThrown(new RuntimeException("Can't reach node")))
                        case (true, request) =>
                          request(id)
                            .foldM(
                              _ => ZIO.fail(TransportError.ExceptionThrown(new RuntimeException("Can't reach node"))),
                              ZIO.succeed(_)
                            )
                      }
                    )
              }
              .toManaged(_.close)

          override def bind(addr: T): ZStream[Any, TransportError, ChunkConnection] =
            for {
              _ <- ZStream.fromEffect(checkIdentity(addr))
              connections <- ZStream.managed(
                              Queue
                                .bounded[ChunkConnection](connectionBuffer)
                                .toManaged(s => s.takeAll.flatMap(ZIO.foreach(_)(_.close)) *> s.shutdown)
                            )
              connect <- ZStream.managed(
                          Ref
                            .make[ConnectionRequest[T]] {
                              remote =>
                                for {
                                  incoming    <- Queue.bounded[Either[Option[TransportError], Chunk[Byte]]](messagesBuffer)
                                  outgoing    <- Queue.bounded[Either[Option[TransportError], Chunk[Byte]]](messagesBuffer)
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
                                           connected.get,
                                           close
                                         )
                                  _ <- connections.offer(con1)
                                  con2 <- makeConnection(
                                           msg => incoming.offer(Right(msg)).unit,
                                           outgoing.take,
                                           inSendLock,
                                           connected.get,
                                           close
                                         )
                                  _ <- ref.update(
                                        old =>
                                          old.copy(
                                            connections = (addr, remote, (e: TransportError) => close(Some(e)).unit) :: old.connections
                                          )
                                      )
                                } yield con2
                            }
                            .toManaged(_.set(_ => ZIO.fail(())))
                        )
              _ <- ZStream.managed(
                    ZManaged.make(
                      ref.update(
                        old => old.copy(nodes = old.nodes + (addr -> ((remote: T) => connect.get.flatMap(_(remote)))))
                      )
                    )(_ => ref.update(old => old.copy(nodes = old.nodes - addr)))
                  )
              _      <- ZStream.fromEffect(completeWaiters(addr))
              result <- ZStream.fromQueue(connections)
            } yield result
        },
        new Service[T] {
          override def awaitAvailable(node: T): UIO[Unit] =
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

          override def existingConnections: UIO[Set[(T, T)]] =
            ref.get.map(_.connections.map { case (t1, t2, _) => (t1, t2) }.toSet)

          override def nodes: UIO[Set[T]] =
            ref.get.map(_.nodes.keySet)

          override def asNode[R1 <: InMemoryTransport[T], E, A](addr: T)(zio: ZIO[R1, E, A]): ZIO[R1, E, A] =
            identityRef.locally(Some(addr))(zio)

          override def setConnectivity(f: (T, T) => Boolean): UIO[Unit] =
            ref
              .modify { old =>
                val (remaining, disconnected) = old.connections.partition { case (t1, t2, _) => f(t1, t2) && f(t2, t1) }
                (disconnected, old.copy(connections = remaining, f = f))
              }
              .flatMap(
                ZIO
                  .foreach_(_)(
                    _._3(TransportError.ExceptionThrown(new RuntimeException("disconnected (setConnectivity)")))
                  )
                  .unit
              )
        }
      )
    }
  }
}
