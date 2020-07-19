package zio.keeper.example

import zio.{ Chunk, ExitCode, Schedule }
import zio.clock.Clock
import zio.console.{ Console, putStrLn }
import zio.keeper.transport.Channel
import zio.logging.Logging
import zio.nio.core.{ InetAddress, SocketAddress }

object UdpServer extends zio.App {
  import zio._
  import zio.keeper.transport._

  val logging = Logging.console((_, msg) => msg)

  val transport = (Clock.live ++ logging) >>> udp.live(128)

  val localEnvironment = Console.live ++ transport

  override def run(args: List[String]) =
    (for {
      localHost <- InetAddress.localHost.orDie
      publicAddress <- SocketAddress
                        .inetSocketAddress(localHost, 8010)
                        .orDie
      console <- ZIO.environment[Console]
      handler = (channel: Channel) => {
        for {
          data <- channel.read
          _    <- putStrLn(new String(data.toArray))
          _    <- channel.send(data)
        } yield ()
      }.catchAll(ex => putStrLn("error: " + ex.msg))
        .provide(console)

      _ <- putStrLn("public address: " + publicAddress.toString())
      _ <- bind(publicAddress)(handler)
            .use(ch => ZIO.never.ensuring(ch.close.ignore))

    } yield ()).ignore.as(ExitCode.success).provideLayer(localEnvironment)
}

object UdpClient extends zio.App {
  import zio.keeper.transport._

  val logging = Logging.console((_, msg) => msg)

  val transport = (Clock.live ++ logging) >>> udp.live(128)

  val localEnvironment = Console.live ++ transport

  override def run(args: List[String]) =
    (for {
      localHost <- InetAddress.localHost.orDie
      publicAddress <- SocketAddress
                        .inetSocketAddress(localHost, 5557)
                        .orDie
      _ <- putStrLn("connect to address: " + publicAddress.toString())
      _ <- connect(publicAddress)
            .use(_.send(Chunk.fromArray("message from client".getBytes)).repeat(Schedule.recurs(100)))
    } yield ()).ignore.as(ExitCode.success).provideLayer(localEnvironment)
}
