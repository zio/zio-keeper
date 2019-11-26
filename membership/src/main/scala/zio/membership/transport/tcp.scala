package zio.membership.transport

import zio._
import zio.nio._
import zio.nio.channels._
import zio.stream._
import zio.duration._
import zio.clock.Clock
import zio.membership.SendError
import java.math.BigInteger
import zio.membership.ReceiveError
import zio.macros.delegate._

object tcp {

  def withTcpTransport(
    maxConnections: Int,
    connectionTimeout: Duration,
    sendTimeout: Duration
  ) = enrichWithM[Transport](tcpTransport(maxConnections, connectionTimeout, sendTimeout))

  def tcpTransport(
    maxConnections: Int,
    connectionTimeout: Duration,
    sendTimeout: Duration
  ): ZIO[Clock, Nothing, Transport] =
    ZIO.environment[Clock].map { env =>
      new Transport {
        val transport: Transport.Service[Any] = new Transport.Service[Any] {
          // TODO: cache connections
          override def send(to: SocketAddress, data: Chunk[Byte]) =
            AsynchronousSocketChannel()
              .use { client =>
                val size = data.size
                for {
                  _ <- client.connect(to)
                  _ <- client.write(Chunk((size >>> 24).toByte, (size >>> 16).toByte, (size >>> 8).toByte, size.toByte))
                  _ <- client.write(data)
                } yield ()
              }
              .mapError(SendError.ExceptionThrown(_))
              .timeoutFail(SendError.Timeout(sendTimeout))(sendTimeout)
              .provide(env)

          override def bind(addr: SocketAddress) =
            ZStream
              .unwrapManaged {
                AsynchronousServerSocketChannel()
                  .flatMap(s => s.bind(addr).toManaged_.as(s))
                  .mapError(ReceiveError.BindFailed(addr, _))
                  .withEarlyRelease
                  .map {
                    case (close, server) =>
                      val messages = ZStream
                        .repeatEffect(server.accept.preallocate)
                        .flatMapPar[Clock, Throwable, Chunk[Byte]](maxConnections) { connection =>
                          ZStream.unwrapManaged {
                            connection.map { channel =>
                              ZStream
                                .repeatEffect(
                                  for {
                                    length <- channel
                                               .read(4)
                                               .flatMap(c => ZIO.effect(new BigInteger(c.toArray).intValue()))
                                    data <- channel.read(length * 8)
                                  } yield data
                                )
                                .timeout(connectionTimeout)
                                .catchAllCause(_ => ZStream.empty)
                            }
                          }
                        }
                        .mapError(ReceiveError.ExceptionThrown)
                      // hack to properly handle interrupts
                      messages.merge(ZStream.never.ensuring(close))
                  }
              }
              .provide(env)
        }
      }
    }
}
