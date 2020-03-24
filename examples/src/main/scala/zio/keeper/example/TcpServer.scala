package zio.keeper.example

import zio.{Chunk, Schedule}
import zio.clock.Clock
import zio.console.{Console, putStrLn}
import zio.keeper.transport.Channel.Connection
import zio.logging.Logging
import zio.nio.core.{InetAddress, SocketAddress}
import zio.duration._

object TcpServer extends zio.App {
  import zio._
  import zio.duration._
  import zio.keeper.transport._

  val logging = Logging.console((_, msg) => msg)

  val transport = (Clock.live ++ logging) >>> tcp.live(10.seconds, 10.seconds)

  val localEnvironment = Console.live ++ transport

  override def run(args: List[String]) =
    (for {
      localHost <- InetAddress.localHost.orDie
      publicAddress <- SocketAddress
        .inetSocketAddress(localHost, 8010)
        .orDie
      console <- ZIO.environment[Console]
      handler = (channel: Connection) => {
        for {
          data <- channel.read
          _    <- putStrLn(new String(data.toArray))
          _    <- channel.send(data)
        } yield ()
        }.forever
        .catchAll(ex => putStrLn("error: " + ex.msg))
        .provide(console)

      _ <- putStrLn("public address: " + publicAddress.toString())
      _ <- bind(publicAddress)(handler)
        .use(ch => ZIO.never.ensuring(ch.close.ignore).unit)
    } yield ()).foldM(e => putStrLn(s"Error: ${e.msg}"), _ => ZIO.unit).as(0).provideLayer(localEnvironment)
}

object TcpClient extends zio.App {
  import zio.keeper.transport._

  val logging = Logging.console((_, msg) => msg)

  val transport = (Clock.live ++ logging) >>> tcp.live(10.seconds, 10.seconds)

  val localEnvironment = Console.live ++ transport

  override def run(args: List[String]) =
    (for {
      localHost <- InetAddress.localHost.orDie
      publicAddress <- SocketAddress
        .inetSocketAddress(localHost, 8010)
        .orDie
      _ <- putStrLn("connect to address: " + publicAddress.toString())
      _ <- connect(publicAddress)
        .use(_.send(Chunk.fromArray("message from client".getBytes)).repeat(Schedule.recurs(100)))
    } yield ()).ignore.as(0).provideLayer(localEnvironment)
}

