package zio.keeper.transport

import java.{ util => ju }

import zio._
import zio.clock.Clock
import zio.keeper.{ NodeAddress, TransportError, uuid }
import zio.keeper.encoding._
import zio.logging.Logging
import zio.logging.log
import zio.nio.channels._
import zio.stream._
import java.io.IOException

object tcp {

  def make[R <: Clock with Logging](
    maxConnections: Long,
    connectionRetryPolicy: Schedule[R, Exception, Any]
  ): ZLayer[R, Nothing, Transport] = ZLayer.fromFunction { env =>
    def toConnection(
      channel: AsynchronousSocketChannel,
      id: ju.UUID,
      _close: UIO[Unit]
    ) =
      for {
        writeLock <- Semaphore.make(1)
        readLock  <- Semaphore.make(1)
        connection = new Connection[Any, Exception, Chunk[Byte], Chunk[Byte]] {
          override def send(dataChunk: Chunk[Byte]) = {
            val size      = dataChunk.size
            val sizeChunk = Chunk.fromArray(intToByteArray(size))

            log.debug(s"$id: Sending $size bytes") *>
              writeLock
                .withPermit {
                  channel
                    .write(sizeChunk ++ dataChunk)
                    .unit
                }
          }.provide(env)

          override val receive = {
            ZStream
              .repeatEffect {
                readLock.withPermit {
                  for {
                    length <- channel.read(4).flatMap(c => byteArrayToInt(c.toArray))
                    data   <- channel.read(length * 8)
                    _      <- log.debug(s"$id: Received $length bytes")
                  } yield data
                }
              }
              .catchAll {
                // zio.nio does not expose -1 from the underlying channel
                case e: IOException if e.getMessage() == "Connection reset by peer" => Stream.empty
                case e                                                              => Stream.fail(e)
              }
          }.provide(env)

          override val close = _close
        }
      } yield connection.mapError(TransportError.ExceptionWrapper(_))

    new Transport.Service {
      override def connect(to: NodeAddress): Managed[TransportError, ChunkConnection] = {
        for {
          id <- uuid.makeRandomUUID.toManaged_
          _  <- log.debug(s"$id: new outbound connection to $to").toManaged_
          connection <- AsynchronousSocketChannel().withEarlyRelease
                         .mapM {
                           case (close, channel) =>
                             val connection = toConnection(
                               channel,
                               id,
                               close.unit
                             )
                             to.socketAddress.flatMap(channel.connect) *> connection
                         }
                         .retry(connectionRetryPolicy)
        } yield connection
      }.provide(env).mapError(TransportError.ExceptionWrapper(_))

      override def bind(addr: NodeAddress): Stream[TransportError, ChunkConnection] = {

        val bind = ZStream.managed {
          AsynchronousServerSocketChannel()
            .tapM { server =>
              addr.socketAddress.flatMap { sAddr =>
                server.bind(sAddr)
              }
            }
            .zip(ZManaged.fromEffect(Semaphore.make(maxConnections)))
        }

        val connections = bind.flatMap {
          case (server, lock) =>
            ZStream.managed(ZManaged.scope).flatMap { allocate =>
              ZStream
                .repeatEffect(
                  allocate(lock.withPermitManaged *> server.accept)
                )
                .mapM {
                  case (close, channel) =>
                    for {
                      id         <- uuid.makeRandomUUID
                      _          <- log.debug(s"$id: new inbound connection")
                      connection <- toConnection(channel, id, close(Exit.unit).unit)
                    } yield connection
                }
            }
        }

        ZStream.fromEffect(log.info(s"Binding transport to $addr")) *> connections
      }.provide(env).mapError(TransportError.ExceptionWrapper(_))
    }
  }
}
