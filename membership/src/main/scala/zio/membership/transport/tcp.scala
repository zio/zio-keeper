package zio.membership.transport

import java.{util => ju}
import java.math.BigInteger

import zio._
import zio.clock.Clock
import zio.duration._
import zio.logging._
import zio.logging.Logging.Logging
import zio.membership.TransportError
import zio.membership.TransportError._
import zio.membership.uuid
import zio.nio.channels._
import zio.stream._

object tcp {

  def live(
    maxConnections: Long,
    connectionTimeout: Duration,
    sendTimeout: Duration,
    retryInterval: Duration = 50.millis
  ): ZLayer[Clock with Logging, Nothing, Transport[Address]] =
    ZLayer.fromFunction { env =>
      def toConnection(channel: AsynchronousSocketChannel, id: ju.UUID) =
        for {
          writeLock <- Semaphore.make(1)
          readLock  <- Semaphore.make(1)
        } yield new Connection[Any, TransportError, Chunk[Byte]] {
          def send(data: Chunk[Byte]): IO[TransportError, Unit] = {
            val size = data.size
            val sizeChunk = Chunk(
              (size >>> 24).toByte,
              (size >>> 16).toByte,
              (size >>> 8).toByte,
              size.toByte
            )

            log.info(s"$id: Sending $size bytes") *>
              writeLock
                .withPermit {
                  channel
                    .write(sizeChunk ++ data)
                    .mapError(ExceptionThrown(_))
                    .timeoutFail(RequestTimeout(sendTimeout))(sendTimeout)
                    .unit
                }
          }.provide(env)

          val receive: Stream[TransportError, Chunk[Byte]] =
            Stream
              .repeatEffect {
                readLock
                  .withPermit {
                    for {
                      length <- channel.read(4).flatMap(c => IO.effect(new BigInteger(c.toArray).intValue()))
                      data   <- channel.read(length * 8)
                      _      <- log.debug(s"$id: Received $length bytes")
                    } yield data
                  }
                  .provide(env)
              }
              .catchAll(_ => Stream.empty)
        }

      new Transport.Service[Address] {
        def bind(addr: Address): Stream[TransportError, Managed[Nothing, ChunkConnection]] = {
          ZStream.fromEffect(log.info(s"Binding transport to $addr")) *>
            ZStream.unwrapManaged {
              AsynchronousServerSocketChannel()
                .tapM(server => addr.toInetSocketAddress.flatMap(server.bind))
                .zip(Managed.fromEffect(Semaphore.make(maxConnections)))
                .mapError(BindFailed(addr, _))
                .withEarlyRelease
                .map {
                  case (close, (server, lock)) =>
                    val accept = ZStream
                      .repeatEffect(server.accept.preallocate)
                      .mapM { channel =>
                        for {
                          id <- uuid.makeRandom
                          _  <- log.debug(s"$id: new inbound connection")
                        } yield (lock.withPermitManaged *> channel).mapM(toConnection(_, id))
                      }
                      .mapError(BindFailed(addr, _))
                    accept.merge(Stream.never.ensuring(close))
                }
            }
        }.provide(env)

        def connect(to: Address): Managed[TransportError, ChunkConnection] = {
          for {
            id <- uuid.makeRandom.toManaged_
            _  <- log.debug(s"$id: new outbound connection to $to").toManaged_
            connection <- AsynchronousSocketChannel()
                           .mapM { channel =>
                             to.toInetSocketAddress.flatMap(channel.connect(_)) *> toConnection(channel, id)
                           }
                           .mapError(ExceptionThrown(_))
                           .retry[Clock, TransportError, Int](Schedule.spaced(retryInterval))
                           .timeout(connectionTimeout)
                           .flatMap {
                             _.fold[Managed[TransportError, ChunkConnection]](
                               Managed.fail(TransportError.ConnectionTimeout(connectionTimeout))
                             )(Managed.succeed(_))
                           }
          } yield connection
        }.provide(env)
      }
    }
}
