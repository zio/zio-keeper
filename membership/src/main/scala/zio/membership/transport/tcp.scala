package zio.membership.transport

import zio._
import zio.nio.channels._
import zio.stream._
import zio.duration._
import zio.clock.Clock
import zio.membership.{ TransportError, uuid }
import TransportError._
import java.math.BigInteger

import zio.logging.Logging
import java.{ util => ju }

import zio.membership.hyparview.ScopeIO

object tcp {

  def make(
    maxConnections: Long,
    connectionTimeout: Duration,
    sendTimeout: Duration,
    retryInterval: Duration = 50.millis
  ): ZLayer[Clock with Logging, Nothing, Transport[Address]] = ZLayer.fromFunction { env =>
    def toConnection(channel: AsynchronousSocketChannel, id: ju.UUID, close0: UIO[Unit]) =
      for {
        writeLock <- Semaphore.make(1)
        readLock  <- Semaphore.make(1)
      } yield {
        new Connection[Any, TransportError, Chunk[Byte]] {
          override def send(dataChunk: Chunk[Byte]): IO[TransportError, Unit] = {
            val size = dataChunk.size
            val sizeChunk = Chunk(
              (size >>> 24).toByte,
              (size >>> 16).toByte,
              (size >>> 8).toByte,
              size.toByte
            )

            logging.logInfo(s"$id: Sending $size bytes") *>
              writeLock
                .withPermit {
                  channel
                    .write(sizeChunk ++ dataChunk)
                    .mapError(ExceptionThrown(_))
                    .timeoutFail(RequestTimeout(sendTimeout))(sendTimeout)
                    .unit
                }
          }.provide(env)
          override val receive: Stream[Nothing, Chunk[Byte]] = {
            ZStream
              .repeatEffect {
                readLock.withPermit {
                  for {
                    length <- channel
                               .read(4)
                               .flatMap(
                                 c => ZIO.effect(new BigInteger(c.toArray).intValue())
                               )
                    data <- channel.read(length * 8)
                    _    <- logging.logDebug(s"$id: Received $length bytes")
                  } yield data
                }
              }
              .catchAll(_ => ZStream.empty)
          }.provide(env)

          override val close: UIO[Unit] = close0
        }
      }

    new Transport.Service[Address] {
      override def connect(to: Address) = {
        for {
          id <- uuid.makeRandomUUID.toManaged_
          _  <- logging.logDebug(s"$id: new outbound connection to $to").toManaged_
          connection <- AsynchronousSocketChannel().withEarlyRelease
                         .mapM {
                           case (close, channel) =>
                             to.toInetSocketAddress
                               .flatMap(channel.connect(_)) *> toConnection(channel, id, close.unit)
                         }
                         .mapError(ExceptionThrown(_))
                         .retry[Clock, TransportError, Int](Schedule.spaced(retryInterval))
                         .timeout(connectionTimeout)
                         .flatMap(
                           _.fold[Managed[TransportError, ChunkConnection]](
                             ZManaged.fail(TransportError.ConnectionTimeout(connectionTimeout))
                           )(ZManaged.succeed(_))
                         )
        } yield connection
      }.provide(env)

      override def bind(addr: Address) = {

        val bind = ZStream.managed {
          AsynchronousServerSocketChannel()
            .tapM(server => addr.toInetSocketAddress.flatMap(server.bind))
            .mapError(BindFailed(addr, _))
            .zip(ZManaged.fromEffect(Semaphore.make(maxConnections)))
        }

        val bindConnection = bind.flatMap {
          case (server, lock) =>
            ZStream.managed(ScopeIO.make).flatMap { allocate =>
              ZStream
                .repeatEffect(allocate(lock.withPermitManaged *> server.accept))
                .mapM {
                  case (channel, close) =>
                    for {
                      id         <- uuid.makeRandomUUID
                      _          <- logging.logDebug(s"$id: new inbound connection")
                      connection <- toConnection(channel, id, close.unit)
                    } yield connection
                }
                .mapError(BindFailed(addr, _))
            }
        }

        ZStream.fromEffect(logging.logInfo(s"Binding transport to $addr")) *> bindConnection
      }.provide(env)
    }
  }
}
