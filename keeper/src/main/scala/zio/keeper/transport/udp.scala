package zio.keeper.transport

import zio._
import zio.clock.Clock
import zio.keeper.TransportError
import zio.keeper.TransportError._
import zio.keeper.transport.Channel._
import zio.logging.Logging.Logging
import zio.logging._
import zio.nio.channels._
import zio.nio.core.{ Buffer, SocketAddress }

object udp {

  /**
   * Creates udp transport with given maximum message size.
   * @param mtu - maximum message size
   * @return layer with Udp transport
   */
  def live(mtu: Int): ZLayer[Clock with Logging, Nothing, Transport] =
    ZLayer.fromFunction { env =>
      new Transport.Service {
        def bind(addr: SocketAddress)(connectionHandler: Connection => UIO[Unit]): Managed[TransportError, Bind] =
          DatagramChannel
            .bind(Some(addr))
            .mapError(BindFailed(addr, _))
            .withEarlyRelease
            .onExit { _ =>
              log.info("shutting down server")
            }
            .mapM {
              case (close, server) =>
                Buffer
                  .byte(mtu)
                  .flatMap(
                    buffer =>
                      server
                        .receive(buffer)
                        .mapError(ExceptionWrapper)
                        .tap(_ => buffer.flip)
                        .map {
                          case Some(addr) =>
                            new Connection(
                              bytes => buffer.getChunk(bytes).mapError(ExceptionWrapper),
                              chunk => Buffer.byte(chunk).flatMap(server.send(_, addr)).mapError(ExceptionWrapper).unit,
                              ZIO.succeed(true),
                              ZIO.unit
                            )
                          case None =>
                            new Connection(
                              bytes => buffer.flip.flatMap(_ => buffer.getChunk(bytes)).mapError(ExceptionWrapper),
                              _ => ZIO.fail(new RuntimeException("Cannot reply")).mapError(ExceptionWrapper).unit,
                              ZIO.succeed(true),
                              ZIO.unit
                            )
                        }
                        .flatMap(
                          connectionHandler
                        )
                  )
                  .forever
                  .fork
                  .as {
                    val local = server.localAddress
                      .flatMap(opt => IO.effect(opt.get).orDie)
                      .mapError(ExceptionWrapper(_))
                    new Bind(server.isOpen, close.unit, local)
                  }
            }
            .provide(env)

        def connect(to: SocketAddress): Managed[TransportError, Connection] =
          DatagramChannel
            .connect(to)
            .mapM(
              channel =>
                Connection.withLock(
                  channel.read(_).mapError(ExceptionWrapper),
                  channel.write(_).mapError(ExceptionWrapper).unit,
                  ZIO.succeed(true),
                  ZIO.unit
                )
            )
            .mapError(ExceptionWrapper)
      }
    }
}
