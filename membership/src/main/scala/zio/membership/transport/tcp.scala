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

object tcp {

  def withTcpTransport(
    connectionTimeout: Duration,
    sendTimeout: Duration
  ) = enrichWithM[Transport[SocketAddress]](tcpTransport(connectionTimeout, sendTimeout))

  def tcpTransport(
    connectionTimeout: Duration,
    sendTimeout: Duration
  ): ZIO[Clock, Nothing, Transport[SocketAddress]] =
    ZIO.environment[Clock].map { env =>

      def toConnection[E](channelManaged: ZManaged[Clock, E, AsynchronousSocketChannel]) =
        channelManaged.withEarlyRelease.flatMap { case (closeChannel, channel) =>
          for {
            writeLock <- Semaphore.make(1).toManaged_
            readLock <- Semaphore.make(1).toManaged_
          } yield {
            new Connection[Any, TransportError, Chunk[Byte]] {
              override def send(data: Chunk[Byte]) = {
                val size = data.size
                writeLock.withPermit(for {
                  _ <- channel.write(Chunk((size >>> 24).toByte, (size >>> 16).toByte, (size >>> 8).toByte, size.toByte))
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
              override val close = closeChannel.unit.provide(env)
            }
          }
        }.provide(env)

      new Transport[SocketAddress] {
        val transport = new Transport.Service[Any, SocketAddress] {

          override def connect(to: SocketAddress) =
            toConnection(AsynchronousSocketChannel().tapM(_.connect(to))).mapError(ExceptionThrown(_))

          override def bind(addr: SocketAddress) =
            ZStream
              .unwrapManaged {
                AsynchronousServerSocketChannel()
                  .flatMap(s => s.bind(addr).toManaged_.as(s))
                  .mapError(BindFailed(addr, _))
                  .withEarlyRelease
                  .map {
                    case (close, server) =>
                      val connections = ZStream
                        .repeatEffect(server.accept.preallocate)
                        .flatMap(channel => ZStream.managed(toConnection(channel)))
                        .mapError(ExceptionThrown(_))

                      // hack to properly handle interrupts
                      connections.merge(ZStream.never.ensuring(close))
                  }
              }
        }
      }
    }
}
