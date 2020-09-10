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
    maxConnections: Int,
    connectRetryPolicy: Schedule[R, Exception, Any]
  ): ZLayer[R, Nothing, Transport] = ZLayer.fromFunctionM { env =>
    def toConnection(
      channel: AsynchronousSocketChannel,
      id: ju.UUID
    ) =
      for {
        writeLock <- Semaphore.make(1)
        readLock  <- Semaphore.make(1)
        connection = new Connection[Any, Exception, Chunk[Byte], Chunk[Byte]] {
          override def send(dataChunk: Chunk[Byte]) = {
            val size      = dataChunk.size
            val sizeChunk = intToByteChunk(size)

            writeLock
              .withPermit {
                channel
                  .write(sizeChunk ++ dataChunk)
                  .unit
              } <* log.debug(s"$id: Sent $size bytes")

          }.provide(env)

          override val receive = {
            ZStream
              .repeatEffect {
                readLock.withPermit {
                  for {
                    length <- channel.read(4).flatMap(byteChunkToInt)
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
        }
      } yield connection.mapError(TransportError.ExceptionWrapper(_))

    Semaphore.make(maxConnections.toLong).map { sema =>
      new Transport.Service {
        override def connect(to: NodeAddress): Managed[TransportError, ChunkConnection] = {
          for {
            _  <- sema.withPermitManaged
            id <- uuid.makeRandomUUID.toManaged_
            _  <- log.debug(s"$id: new outbound connection to $to").toManaged_
            connection <- AsynchronousSocketChannel()
                           .mapM { channel =>
                             val connection = toConnection(channel, id)
                             to.socketAddress.flatMap(channel.connect) *> connection
                           }
                           .retry(connectRetryPolicy)
          } yield connection
        }.provide(env).mapError(TransportError.ExceptionWrapper(_))

        override def bind(addr: NodeAddress): Stream[TransportError, ChunkConnection] =
          ZStream
            .managed {
              AsynchronousServerSocketChannel()
                .tapM { server =>
                  addr.socketAddress.flatMap { sAddr =>
                    log.info(s"Binding transport to $sAddr") *> server.bind(sAddr)
                  }
                }
            }
            .flatMap { server =>
              ZStream.managed {
                for {
                  _          <- sema.withPermitManaged
                  channel    <- server.accept
                  id         <- uuid.makeRandomUUID.toManaged_
                  _          <- log.debug(s"$id: new inbound connection").toManaged_
                  connection <- toConnection(channel, id).toManaged_
                } yield connection
              }.forever
            }
            .provide(env)
            .mapError(TransportError.ExceptionWrapper(_))
      }
    }
  }
}
