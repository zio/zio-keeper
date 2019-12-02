package zio.membership.transport

import zio._
import zio.nio._
import zio.nio.channels._
import zio.stream._
import zio.duration._
import zio.clock.Clock
import zio.membership.{ BindFailed, ExceptionThrown, RequestTimeout, TransportError }
import java.math.BigInteger
import zio.macros.delegate._
import zio.membership.RequestTimeout
import zio.membership.MaxConnectionsReached

object tcp {

  def withTcpTransport(
    maxConnections: Int,
    connectionTimeout: Duration,
    sendTimeout: Duration
  ) = enrichWithManaged[Transport[SocketAddress]](tcpTransport(maxConnections, connectionTimeout, sendTimeout))

  def tcpTransport(
    maxConnections: Int,
    connectionTimeout: Duration,
    sendTimeout: Duration
  ): ZManaged[Clock, Nothing, Transport[SocketAddress]] =
    ZManaged {
      for {
        env       <- ZIO.environment[Clock]
        nextId    <- Ref.make(-1).map(_.update(_ + 1))
        finalizer <- Ref.make(Map.empty[Int, Exit[_, _] => UIO[_]])
      } yield {
        def toConnection(channelManaged: ZManaged[Clock, TransportError, AsynchronousSocketChannel]) =
          (for {
            writeLock <- Semaphore.make(1)
            readLock  <- Semaphore.make(1)
            id        <- nextId
          } yield {
            val managed = channelManaged.withEarlyRelease
              .map {
                case (closeChannel, channel) =>
                  new Connection[Any, TransportError, Chunk[Byte]] {
                    override def send(data: Chunk[Byte]) = {
                      val size = data.size
                      writeLock
                        .withPermit(for {
                          _ <- channel.write(
                                Chunk((size >>> 24).toByte, (size >>> 16).toByte, (size >>> 8).toByte, size.toByte)
                              )
                          _ <- channel.write(data)
                        } yield ())
                        .mapError(ExceptionThrown(_))
                        .timeoutFail(RequestTimeout(sendTimeout))(sendTimeout)
                        .provide(env)
                    }
                    override val receive =
                      ZStream
                        .repeatEffect(
                          readLock.withPermit(for {
                            length <- channel
                                       .read(4)
                                       .flatMap(c => ZIO.effect(new BigInteger(c.toArray).intValue()))
                            data <- channel.read(length * 8)
                          } yield data)
                        )
                        .timeout(connectionTimeout)
                        .catchAllCause(_ => ZStream.empty)
                        .provide(env)
                    override val close =
                      (closeChannel.ignore.provide(env) *> finalizer.update(_ - id)).uninterruptible.unit
                  }
              }
              .provide(env)

            for {
              overCapacity <- finalizer.get.map(_.size >= maxConnections) // it's fine to go over maximum for a short time
              connection <- if (overCapacity) ZIO.fail(MaxConnectionsReached(maxConnections)) else {
                ZIO.uninterruptibleMask { restore =>
                  for {
                    res        <- managed.reserve
                    connection <- restore(res.acquire).onError(c => res.release(Exit.halt(c)))
                    _          <- finalizer.update(_ + (id -> res.release))
                  } yield connection
                }
              }
            } yield connection
          }).flatten

        Reservation(
          ZIO.succeed {
            new Transport[SocketAddress] {
              val transport = new Transport.Service[Any, SocketAddress] {

                override def connect(to: SocketAddress) =
                  toConnection(AsynchronousSocketChannel().tapM(_.connect(to)).mapError(ExceptionThrown(_)))

                override def bind(addr: SocketAddress) =
                  ZStream
                    .unwrapManaged {
                      AsynchronousServerSocketChannel()
                        .flatMap(s => s.bind(addr).toManaged_.as(s))
                        .mapError(BindFailed(addr, _))
                        .map { server =>
                          ZStream
                            .repeatEffect(toConnection(server.accept.mapError(BindFailed(addr, _))))
                        }
                    }
              }
            }
          },
          exitU =>
            for {
              fs    <- finalizer.get.map(_.values)
              exits <- ZIO.foreach(fs)(_(exitU).run)
              _     <- ZIO.done(Exit.collectAll(exits).getOrElse(Exit.unit))
            } yield ()
        )
      }
    }
}
