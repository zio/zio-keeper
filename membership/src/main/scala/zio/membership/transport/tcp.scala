package zio.membership.transport

import zio._
import zio.nio.channels._
import zio.stream._
import zio.duration._
import zio.clock.Clock
import zio.membership.TransportError
import TransportError._
import java.math.BigInteger
import zio.macros.delegate._

object tcp {

  def withTcpTransport(
    maxConnections: Long,
    connectionTimeout: Duration,
    sendTimeout: Duration,
    retryInterval: Duration = 50.millis
  ) =
    enrichWithM[Transport[Address]](
      tcpTransport(
        maxConnections,
        connectionTimeout,
        sendTimeout,
        retryInterval
      )
    )

  def tcpTransport(
    maxConnections: Long,
    connectionTimeout: Duration,
    sendTimeout: Duration,
    retryInterval: Duration = 50.millis
  ): URIO[Clock, Transport[Address]] = ZIO.access[Clock] { env =>
    def toConnection(channel: AsynchronousSocketChannel) =
      for {
        writeLock <- Semaphore.make(1)
        readLock  <- Semaphore.make(1)
      } yield {
        new Connection[Any, TransportError, Chunk[Byte]] {
          def send(dataChunk: Chunk[Byte]) = {
            val size = dataChunk.size
            val sizeChunk = Chunk(
              (size >>> 24).toByte,
              (size >>> 16).toByte,
              (size >>> 8).toByte,
              size.toByte
            )

            writeLock
              .withPermit {
                channel
                  .write(sizeChunk ++ dataChunk)
                  .mapError(ExceptionThrown(_))
                  .timeoutFail(RequestTimeout(sendTimeout))(sendTimeout)
                  .unit
              }
              .provide(env)
          }
          val receive =
            ZStream
              .repeatEffect(readLock.withPermit(for {
                length <- channel
                           .read(4)
                           .flatMap(
                             c => ZIO.effect(new BigInteger(c.toArray).intValue())
                           )
                data <- channel.read(length * 8)
              } yield data))
              .catchAll(_ => ZStream.empty)
        }
      }

    new Transport[Address] {
      val transport = new Transport.Service[Any, Address] {
        override def connect(to: Address) =
          AsynchronousSocketChannel()
            .mapM { channel =>
              to.toInetSocketAddres.flatMap(channel.connect(_)) *> toConnection(channel)
            }
            .mapError(ExceptionThrown(_))
            .retry[Clock, TransportError, Int](Schedule.spaced(retryInterval))
            .timeout(connectionTimeout)
            .flatMap(
              _.fold[Managed[TransportError, ChunkConnection]](
                ZManaged.fail(TransportError.ConnectionTimeout(connectionTimeout))
              )(ZManaged.succeed)
            )
            .provide(env)

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
                    .bimap(
                      BindFailed(addr, _),
                      c => (lock.withPermitManaged *> c).mapM(toConnection)
                    )
                  accept.merge(ZStream.never.ensuring(close))
              }
          }
      }
    }
  }
}
