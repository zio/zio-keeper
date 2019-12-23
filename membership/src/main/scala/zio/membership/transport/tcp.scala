package zio.membership.transport

import zio._
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
    maxConnections: Long,
    connectionTimeout: Duration,
    sendTimeout: Duration
  ) = enrichWithM[Transport[Address]](tcpTransport(maxConnections, connectionTimeout, sendTimeout))

  def tcpTransport(
    maxConnections: Long,
    connectionTimeout: Duration,
    sendTimeout: Duration
  ): URIO[Clock, Transport[Address]] = ZIO.access[Clock] { env =>
    def toConnection(channel: AsynchronousSocketChannel) =
      for {
        writeLock <- Semaphore.make(1)
        readLock  <- Semaphore.make(1)
      } yield {
        new Connection[Any, TransportError, Chunk[Byte]] {
          def send(data: Chunk[Byte]) = {
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
          val receive =
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
        }
      }

    new Transport[Address] {
      val transport = new Transport.Service[Any, Address] {
        override def connect(to: Address) =
          AsynchronousSocketChannel()
            .mapM(channel => to.toInetSocketAddres.flatMap(channel.connect(_)) *> toConnection(channel))
            .mapError(ExceptionThrown(_))

        override def bind(addr: Address) =
          ZStream.unwrapManaged {
            AsynchronousServerSocketChannel()
              .tapM(server => addr.toInetSocketAddres.flatMap(server.bind))
              .zip(ZManaged.fromEffect(Semaphore.make(maxConnections)))
              .mapError(BindFailed(addr, _))
              .withEarlyRelease
              .map {
                case (close, (server, lock)) =>
                  val accept = ZStream
                    .repeatEffect(server.accept.preallocate)
                    .flatMap(c => ZStream.managed(lock.withPermitManaged).as(c))
                    .bimap(BindFailed(addr, _), _.mapM(toConnection))

                  accept.merge(ZStream.never.ensuring(close))
              }
          }
      }
    }
  }
}
