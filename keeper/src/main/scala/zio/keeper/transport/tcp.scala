package zio.keeper.transport

import java.math.BigInteger

import zio._
import zio.clock.Clock
import zio.console.Console
import zio.duration._
import zio.keeper.Error.ConnectionTimeout
import zio.keeper.{ BindFailed, ExceptionThrown, RequestTimeout, TransportError }
import zio.macros.delegate._
import zio.nio._
import zio.nio.channels._

object tcp {

  def withTcpTransport(
    connectionTimeout: Duration,
    sendTimeout: Duration
  ) = enrichWithM[Transport](tcpTransport(connectionTimeout, sendTimeout))

  def tcpTransport(
    connectionTimeout: Duration,
    requestTimeout: Duration
  ): ZIO[Clock with Console, Nothing, Transport] =
    ZIO.environment[Clock with Console].map { env =>
      new Transport {
        val transport = new Transport.Service[Any] {
          override def connect(to: SocketAddress) =
            (for {
              socketChannelAndClose  <- AsynchronousSocketChannel().withEarlyRelease
              (close, socketChannel) = socketChannelAndClose
              _ <- socketChannel
                    .connect(to)
                    .timeoutFail(ConnectionTimeout(to, requestTimeout))(requestTimeout)
                    .toManaged_
            } yield new NioChannelOut(socketChannel, requestTimeout, close, env))
              .mapError(ExceptionThrown(_))
              .provide(env)

          override def bind(addr: SocketAddress)(connectionHandler: ChannelOut => ZIO[Any, Nothing, Unit]) =
            AsynchronousServerSocketChannel()
              .flatMap(s => s.bind(addr).toManaged_.as(s))
              .mapError(BindFailed(addr, _))
              .withEarlyRelease
              .mapM {
                case (close, server) =>
                  (for {
                    cur <- server.accept.withEarlyRelease
                            .map { case (close, socket) => new NioChannelOut(socket, connectionTimeout, close, env) }
                            .mapError(ExceptionThrown)
                            .preallocate
                            .race(
                              ZIO.never.ensuring(close)
                            )
                    _ <- cur.use(connectionHandler).fork
                  } yield ()).forever.fork
                    .as(new NioChannelIn(server, close))
              }
        }
      }
    }
}

class NioChannelOut(
  socket: AsynchronousSocketChannel,
  requestTimeout: Duration,
  finalizer: URIO[Any, Any],
  clock: Clock
) extends ChannelOut {

  override def send(data: Chunk[Byte]): ZIO[Any, TransportError, Unit] = {
    val size = data.size
    (for {
      _ <- socket.write(Chunk((size >>> 24).toByte, (size >>> 16).toByte, (size >>> 8).toByte, size.toByte))
      _ <- socket.write(data)
    } yield ())
      .mapError(ExceptionThrown(_))
      .timeoutFail(RequestTimeout(requestTimeout))(requestTimeout)
      .provide(clock)
  }

  override def read: ZIO[Any, TransportError, Chunk[Byte]] =
    (for {
      length <- socket
                 .read(4)
                 .flatMap(c => ZIO.effect(new BigInteger(c.toArray).intValue()))
      data <- socket.read(length)
    } yield data).mapError(ExceptionThrown)

  override def isOpen: ZIO[Any, TransportError, Boolean] =
    socket.isOpen

  override def close: ZIO[Any, TransportError, Unit] =
    finalizer.ignore
}

class NioChannelIn(
  serverSocket: AsynchronousServerSocketChannel,
  finalizer: URIO[Any, Any]
) extends ChannelIn {

  override def isOpen: ZIO[Any, TransportError, Boolean] =
    serverSocket.isOpen

  override def close: ZIO[Any, TransportError, Unit] =
    finalizer.ignore

}
