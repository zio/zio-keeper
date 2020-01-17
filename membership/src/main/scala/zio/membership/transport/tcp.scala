package zio.membership.transport

import zio._
import zio.nio.channels._
import zio.stream._
import zio.duration._
import zio.clock.Clock
import zio.membership.TransportError
import zio.membership.log
import TransportError._
import java.math.BigInteger
import zio.macros.delegate._
import zio.logging.Logging
import java.{ util => ju }
import zio.membership.uuid

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
  ): URIO[Clock with Logging[String], Transport[Address]] = ZIO.access { env =>
    def toConnection(channel: AsynchronousSocketChannel, id: ju.UUID) =
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

            log.info(s"$id: Sending $size bytes") *>
              writeLock
                .withPermit {
                  channel
                    .write(sizeChunk ++ dataChunk)
                    .mapError(ExceptionThrown(_))
                    .timeoutFail(RequestTimeout(sendTimeout))(sendTimeout)
                    .unit
                }
          }.provide(env)
          val receive = {
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
                    _    <- log.debug(s"$id: Received $length bytes")
                  } yield data
                }
              }
              .catchAll(_ => ZStream.empty)
          }.provide(env)
        }
      }

    new Transport[Address] {
      val transport = new Transport.Service[Any, Address] {
        override def connect(to: Address) = {
          for {
            id <- uuid.makeRandom.toManaged_
            _  <- log.debug(s"$id: new outbound connection to $to").toManaged_
            connection <- AsynchronousSocketChannel()
                           .mapM { channel =>
                             to.toInetSocketAddres.flatMap(channel.connect(_)) *> toConnection(channel, id)
                           }
                           .mapError(ExceptionThrown(_))
                           .retry[Clock, TransportError, Int](Schedule.spaced(retryInterval))
                           .timeout(connectionTimeout)
                           .flatMap(
                             _.fold[Managed[TransportError, ChunkConnection]](
                               ZManaged.fail(TransportError.ConnectionTimeout(connectionTimeout))
                             )(ZManaged.succeed)
                           )
          } yield connection
        }.provide(env)

        override def bind(addr: Address) = {
          ZStream.fromEffect(log.info(s"Binding transport to $addr")) *>
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
                      .mapM { channel =>
                        for {
                          id <- uuid.makeRandom
                          _  <- log.debug(s"$id: new inbound connection")
                        } yield (lock.withPermitManaged *> channel).mapM(toConnection(_, id))
                      }
                      .mapError(BindFailed(addr, _))
                    accept.merge(ZStream.never.ensuring(close))
                }
            }
        }.provide(env)
      }
    }
  }
}
