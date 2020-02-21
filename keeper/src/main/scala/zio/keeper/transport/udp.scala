package zio.keeper.transport

import java.io.IOException

import zio._
import zio.clock.Clock
import zio.duration.Duration
import zio.keeper.TransportError
import zio.keeper.TransportError._
import zio.logging.Logging
import zio.macros.delegate._
import zio.nio.SocketAddress
import zio.nio.channels.DatagramChannel

object udp {

  def withUdpTransport(
    connectionTimeout: Duration,
    requestTimeout: Duration,
    chunkSize: Int
  ) = enrichWithM[Transport](udpTransport(connectionTimeout, requestTimeout, chunkSize))

  def udpTransport(
    connectionTimeout: Duration,
    requestTimeout: Duration,
    chunkSize: Int
  ): ZIO[Clock with Logging[String], Nothing, Transport] = {
    val connectionTimeout_ = connectionTimeout
    val requestTimeout_ = requestTimeout
    val chunkSize_ = chunkSize
    ZIO.environment[Clock with Logging[String]].map { env =>
      new Live with Clock {
        override val connectionTimeout: Duration = connectionTimeout_
        override val requestTimeout: Duration = requestTimeout_
        override val logger: Logging.Service[Any, String] = env.logging
        override val clock: Clock.Service[Any] = env.clock
        override val chunkSize: Int = chunkSize_
      }
    }
  }

  trait Live extends Transport { self: Clock =>
    val connectionTimeout: Duration
    val requestTimeout: Duration
    val chunkSize: Int

    val logger: Logging.Service[Any, String]

    val transport = new Transport.Service[Any] {

      override def connect(to: SocketAddress): ZManaged[Any, TransportError, ChannelOut] =
        (for {
          datagramChannelAndClose  <- DatagramChannel().withEarlyRelease.mapError(ExceptionWrapper)
          (close, datagramChannel) = datagramChannelAndClose
          _ <- datagramChannel
                .connect(to)
                .mapError(ExceptionWrapper)
                .timeoutFail(ConnectionTimeout(to, connectionTimeout))(connectionTimeout)
                .toManaged_
          _ <- logger.info("transport connected to " + to).toManaged_
        } yield new DatagramChannelOut(datagramChannel, to, requestTimeout, chunkSize, close, self))
          .provide(self)

      override def bind(
        addr: SocketAddress
      )(connectionHandler: ChannelOut => zio.UIO[Unit]): ZManaged[Any, TransportError, ChannelIn] =
        // There is no way to use the datagram channel as a `ChannelOut` because
        // it is not connected to any remote address (read on `ChannelOut` cannot be called).
        // Also, UDP datagram channels are used with `receive`, instead of `read`
        // because one does not need to necessarily care where the data comes from.
        // Calling `read` on a connected datagram channel only allows receiving bytes from
        // the remote address.
        DatagramChannel()
          .flatMap(dc => dc.bind(addr).toManaged_.as(dc))
          .mapError(BindFailed(addr, _))
          .withEarlyRelease
          .onExit { _ =>
            logger.info("shutting down udp channel")
          }
          .map {
            case (close, datagramChannel) =>
              new DatagramChannelIn(datagramChannel, close)
          }
    }
  }

  private class DatagramChannelOut(
    datagramChannel: DatagramChannel,
    remoteAddress: SocketAddress,
    requestTimeout: Duration,
    chunkSize: Int,
    finalizer: URIO[Any, Any],
    clock: Clock
  ) extends ChannelOut {

    override def isOpen: ZIO[Any, TransportError, Boolean] =
      datagramChannel.isOpen

    override def read: ZIO[Any, TransportError, Chunk[Byte]] =
      datagramChannel
        .read(chunkSize)
        .map(_.fold[Chunk[Byte]](Chunk.empty)(identity))
        .mapError(ExceptionWrapper)
        .catchSome {
          case ExceptionWrapper(ex: IOException) if ex.getMessage == "Connection reset by peer" =>
            close *> IO.fail(ExceptionWrapper(ex))
        }

    override def send(data: Chunk[Byte]): ZIO[Any, TransportError, Unit] =
      datagramChannel
        .write(data)
        .retry(Schedule.recurs(3))
        .mapError(ExceptionWrapper)
        .unit
        .catchSome {
          case ExceptionWrapper(ex: IOException) if ex.getMessage == "Connection reset by peer" =>
            close *> IO.fail(ExceptionWrapper(ex))
        }
        .timeoutFail(RequestTimeout(remoteAddress, requestTimeout))(requestTimeout)
        .provide(clock)

    override def close: ZIO[Any, TransportError, Unit] =
      finalizer.ignore
  }

  private class DatagramChannelIn(
    datagramChannel: DatagramChannel,
    finalizer: URIO[Any, Any]
  ) extends ChannelIn {

    override def close: ZIO[Any, TransportError, Unit] =
      finalizer.ignore

    override def isOpen: ZIO[Any, TransportError, Boolean] =
      datagramChannel.isOpen
  }
}
