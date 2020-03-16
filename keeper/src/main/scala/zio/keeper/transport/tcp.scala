package zio.keeper.transport

import java.io.IOException
import java.math.BigInteger

import zio._
import zio.clock.Clock
import zio.duration._
import zio.keeper.TransportError
import zio.keeper.TransportError._
import zio.logging
import zio.logging.Logging
import zio.nio.core.SocketAddress
import zio.nio.channels._

object tcp {

  def live(
    connectionTimeout: Duration,
    requestTimeout: Duration
  ): ZLayer[Clock with Logging, Nothing, Transport] = {
    val connectionTimeout_ = connectionTimeout
    val requestTimeout_    = requestTimeout
    ZLayer.fromFunction { env =>
      new TcpService {
        val connectionTimeout = connectionTimeout_
        val requestTimeout    = requestTimeout_
        val environment       = env
      }
    }
  }

  private trait TcpService extends Transport.Service {
    val connectionTimeout: Duration
    val requestTimeout: Duration
    val environment: Clock with Logging

    def bind(addr: SocketAddress)(connectionHandler: ChannelOut => UIO[Unit]): Managed[TransportError, ChannelIn] =
      AsynchronousServerSocketChannel()
        .flatMap(s => s.bind(addr).toManaged_.as(s))
        .mapError(BindFailed(addr, _))
        .withEarlyRelease
        .onExit { _ =>
          logging.logInfo("shutting down server")
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
                              logging
                                .logInfo(s"connection accepted from: $addr")
                                .as(
                                  new NioChannelOut(socket, addr, connectionTimeout, close, environment)
                                )
                          }
                      }
                      .preallocate

              _ <- cur.use(connectionHandler).fork
            } yield ()).forever.fork
              .as(new NioChannelIn(server, close))
        }
        .provide(environment)

    def connect(to: SocketAddress): Managed[TransportError, ChannelOut] = {
      for {
        socketChannelAndClose  <- AsynchronousSocketChannel().withEarlyRelease.mapError(ExceptionWrapper)
        (close, socketChannel) = socketChannelAndClose
        _ <- socketChannel
              .connect(to)
              .mapError(ExceptionWrapper)
              .timeoutFail(ConnectionTimeout(to, connectionTimeout))(connectionTimeout)
              .toManaged_
        _ <- logging.logInfo(s"transport connected to $to").toManaged_
      } yield new NioChannelOut(socketChannel, to, requestTimeout, close, environment)
    }.provide(environment)
  }

  private class NioChannelOut(
    socket: AsynchronousSocketChannel,
    remoteAddress: SocketAddress,
    requestTimeout: Duration,
    finalizer: URIO[Any, Any],
    environment: Clock
  ) extends ChannelOut {

    def isOpen: IO[TransportError, Boolean] =
      socket.isOpen

    def read: IO[TransportError, Chunk[Byte]] =
      (for {
        length <- socket
                   .read(4)
                   .flatMap(c => ZIO.effect(new BigInteger(c.toArray).intValue()))
                   .mapError(ExceptionWrapper)
        data <- socket.read(length).mapError(ExceptionWrapper)
      } yield data)
        .catchSome {
          case ExceptionWrapper(ex: IOException) if ex.getMessage == "Connection reset by peer" =>
            close *> ZIO.fail(ExceptionWrapper(ex))
        }

    def send(data: Chunk[Byte]): IO[TransportError, Unit] = {
      val size = data.size
      (for {
        _ <- socket
              .write(Chunk((size >>> 24).toByte, (size >>> 16).toByte, (size >>> 8).toByte, size.toByte))
              .mapError(ExceptionWrapper(_))
        _ <- socket.write(data).retry(Schedule.recurs(3)).mapError(ExceptionWrapper(_))
      } yield ())
        .catchSome {
          case ExceptionWrapper(ex: IOException) if ex.getMessage == "Connection reset by peer" =>
            close *> ZIO.fail(ExceptionWrapper(ex))
        }
        .timeoutFail(RequestTimeout(remoteAddress, requestTimeout))(requestTimeout)
        .provide(environment)
    }

    def close: IO[TransportError, Unit] =
      finalizer.ignore
  }

  private class NioChannelIn(serverSocket: AsynchronousServerSocketChannel, finalizer: URIO[Any, Any])
      extends ChannelIn {

    def close: IO[TransportError, Unit] =
      finalizer.ignore

    def isOpen: IO[TransportError, Boolean] =
      serverSocket.isOpen
  }
}
