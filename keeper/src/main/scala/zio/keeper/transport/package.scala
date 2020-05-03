package zio.keeper

import zio.nio.core.SocketAddress
import zio.{ Chunk, Has, ZIO, ZManaged }

package object transport {

  type ConnectionLessTransport = Has[ConnectionLessTransport.Service]
  type Transport               = Has[Transport.Service]
  type ChunkConnection         = Connection[Any, TransportError, Chunk[Byte]]

  def bind[R <: ConnectionLessTransport](
    localAddr: SocketAddress
  )(
    connectionHandler: Channel => ZIO[R, Nothing, Unit]
  ): ZManaged[R, TransportError, Bind] =
    ZManaged
      .environment[R]
      .flatMap(
        env =>
          env
            .get[ConnectionLessTransport.Service]
            .bind(localAddr)(
              conn => connectionHandler(conn).provide(env)
            )
      )

  def connect(to: SocketAddress): ZManaged[ConnectionLessTransport, TransportError, Channel] =
    ZManaged.environment[ConnectionLessTransport].flatMap(_.get.connect(to))
}
