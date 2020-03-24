package zio.keeper

import zio.keeper.transport.Channel._
import zio.nio.core.SocketAddress
import zio.{ Has, ZIO, ZManaged }

package object transport {

  type Transport = Has[Transport.Service]

  def bind[R](
    localAddr: SocketAddress
  )(
    connectionHandler: Connection => ZIO[R, Nothing, Unit]
  ): ZManaged[Transport with R, TransportError, Bind] =
    ZManaged
      .environment[Transport with R]
      .flatMap(
        env =>
          env
            .get[Transport.Service]
            .bind(localAddr)(
              conn => connectionHandler(conn).provide(env)
            )
      )

  def connect(to: SocketAddress): ZManaged[Transport, TransportError, Connection] =
    ZManaged.environment[Transport].flatMap(_.get.connect(to))
}
