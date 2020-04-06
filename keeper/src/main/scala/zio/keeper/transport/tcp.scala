package zio.keeper.transport

import java.io.IOException

import zio._
import zio.clock.Clock
import zio.duration._
import zio.keeper.TransportError
import zio.keeper.TransportError._
import zio.keeper.transport.Channel._
import zio.logging.Logging.Logging
import zio.logging.log
import zio.nio.channels._
import zio.nio.core.SocketAddress

object tcp {

  /**
   * Creates layer with tcp transport.
   * @param connectionTimeout connection timeout
   * @param requestTimeout request timeout
   * @return layer with tcp transport.
   */
  def live(
    connectionTimeout: Duration,
    requestTimeout: Duration
  ): ZLayer[Clock with Logging, Nothing, Transport] =
    ZLayer.fromFunction { env =>
      new Transport.Service {
        def bind(addr: SocketAddress)(connectionHandler: Connection => UIO[Unit]): Managed[TransportError, Bind] =
          AsynchronousServerSocketChannel()
            .flatMap(s => s.bind(addr).toManaged_.as(s))
            .mapError(BindFailed(addr, _))
            .withEarlyRelease
            .onExit { _ =>
              log.info("shutting down server")
            }
            .mapM {
              case (close, server) =>
                (for {
                  cur <- server.accept.withEarlyRelease
                          .mapError(ExceptionWrapper)
                          .mapM {
                            case (close, socket) =>
                              socket.remoteAddress.flatMap {
                                case None =>
                                  // This is almost impossible here but still we need to handle it.
                                  ZIO.fail(ExceptionWrapper(new RuntimeException("cannot obtain address")))
                                case Some(addr) =>
                                  log
                                    .info(s"connection accepted from: $addr")
                                    .flatMap(_ => createConnection(socket, addr, requestTimeout, close.unit))
                              }
                          }
                          .preallocate

                  _ <- cur.use(connectionHandler).fork
                } yield ()).forever.fork
                  .as {
                    val local = server.localAddress
                      .flatMap(opt => IO.effect(opt.get).orDie)
                      .mapError(ExceptionWrapper(_))
                    new Bind(server.isOpen, close.unit, local)
                  }
            }
            .provide(env)

        def connect(to: SocketAddress): Managed[TransportError, Connection] = {
          for {
            socketChannelAndClose  <- AsynchronousSocketChannel().withEarlyRelease.mapError(ExceptionWrapper)
            (close, socketChannel) = socketChannelAndClose
            _ <- socketChannel
                  .connect(to)
                  .mapError(ExceptionWrapper)
                  .timeoutFail(ConnectionTimeout(to, connectionTimeout))(connectionTimeout)
                  .toManaged_
            _ <- log.info(s"transport connected to $to").toManaged_
            connection <- createConnection(
                           socketChannel,
                           to,
                           requestTimeout,
                           close.unit
                         ).toManaged(_.close.ignore)
          } yield connection
        }.provide(env)
      }
    }

  private def createConnection(
    socketChannel: AsynchronousSocketChannel,
    to: SocketAddress,
    requestTimeout: Duration,
    close: IO[TransportError, Unit]
  ): URIO[Clock, Connection] = {
    def handleConnectionReset[A]: PartialFunction[zio.keeper.TransportError, IO[zio.keeper.TransportError, A]] = {
      case ExceptionWrapper(ex: IOException) if ex.getMessage == "Connection reset by peer" =>
        close *> ZIO.fail(ExceptionWrapper(ex))
    }
    ZIO.accessM[Clock] { env =>
      Connection.withLock(
        socketChannel
          .read(_)
          .mapError(ExceptionWrapper)
          .catchSome(handleConnectionReset),
        socketChannel
          .write(_)
          .mapError(ExceptionWrapper)
          .catchSome(handleConnectionReset)
          .timeoutFail(RequestTimeout(to, requestTimeout))(requestTimeout)
          .unit
          .provide(env),
        socketChannel.isOpen,
        close
      )
    }
  }
}
