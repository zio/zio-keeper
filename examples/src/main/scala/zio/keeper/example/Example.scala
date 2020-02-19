package zio.keeper.example

import upickle.default._
import zio.clock._
import zio.console._
import zio.duration._
import zio.keeper.discovery.Discovery
import zio.keeper.example.TestNode.PingPong.Pong
import zio.keeper.membership.{Membership, NodeAddress}
import zio.keeper.membership.swim.SWIM
import zio.keeper.transport.Transport
import zio.keeper.{ByteCodec, TaggedCodec, transport}
import zio.logging.Logging
import zio.logging.slf4j.Slf4jLogger
import zio.macros.delegate._
import zio.macros.delegate.syntax._
import zio.nio.core.{InetAddress, SocketAddress}
import zio.{ZIO, ZManaged}

object Node1 extends zio.ManagedApp {

  def run(args: List[String]) =
    TestNode.start(5557, Set.empty)
}

object Node2 extends zio.ManagedApp {

  def run(args: List[String]) =
    TestNode.start(5558, Set(5557))
}

object Node3 extends zio.ManagedApp {

  def run(args: List[String]) =
    TestNode.start(5559, Set(5558))
}

object TestNode {

  val loggingEnv = ZIO.environment[zio.ZEnv] @@ enrichWith[Logging[String]](
    new Slf4jLogger.Live {

      override def formatMessage(msg: String): ZIO[Any, Nothing, String] =
        ZIO.succeed(msg)
    }
  )

  val tcp =
    loggingEnv >>>
      ZIO.environment[zio.ZEnv with Logging[String]] @@
        transport.tcp.withTcpTransport(10.seconds, 10.seconds)

  sealed trait PingPong

  object PingPong {
    case object Ping extends PingPong
    case object Pong extends PingPong

    implicit val pingCodec: ByteCodec[Ping.type] =
      ByteCodec.fromReadWriter(macroRW[Ping.type])

    implicit val pongCodec: ByteCodec[Pong.type] =
      ByteCodec.fromReadWriter(macroRW[Pong.type])

    implicit def tagged(implicit p1: ByteCodec[Ping.type], p2: ByteCodec[Pong.type]) =
      TaggedCodec.instance[PingPong]({
        case Pong => 1
        case Ping => 2
      }, {
        case 1 => p1.asInstanceOf[ByteCodec[PingPong]]
        case 2 => p2.asInstanceOf[ByteCodec[PingPong]]
      })
  }

  def start(port: Int, otherPorts: Set[Int]) =
    (environment(port, otherPorts) >>> (for {
      env <- ZManaged.environment[Membership[NodeAddress, PingPong]]
      _            <- sleep(5.seconds).toManaged_
//      _ <- broadcast(Chunk.fromArray(nodeName.getBytes)).ignore.toManaged_
      _ <- env.membership.receive.foreach {
            case (sender, message) =>
              putStrLn("receive message: " + message) *> env.membership.send(Pong, sender).ignore *> sleep(5.seconds)
          }.toManaged_
    } yield ()))
      .fold(ex => {
        println(s"exit with error: $ex")
        1
      }, _ => 0)

  private def environment(port: Int, others: Set[Int]) =
    discoveryEnv(others).toManaged_ @@
      enrichWithManaged(tcp.toManaged_) >>>
      membership(port)

  def discoveryEnv(others: Set[Int]) =
    ZIO.environment[zio.ZEnv] @@
      enrichWithM[Discovery](
        ZIO
          .foreach(others)(
            port => InetAddress.localHost.flatMap(addr => SocketAddress.inetSocketAddress(addr, port))
          )
          .orDie
          .map(addrs => Discovery.staticList(addrs.toSet))
      )

  def membership(port: Int) =
    ZManaged.environment[zio.ZEnv with Logging[String] with Transport with Discovery] @@
      enrichWithManaged(
        SWIM.run[PingPong](port)
      )
}
//
//object TcpServer extends zio.App {
//  import zio._
//  import zio.duration._
//  import zio.keeper.transport._
//
//  val localEnvironment = ZIO.environment[zio.ZEnv] @@ enrichWith[Logging[String]](
//    new Slf4jLogger.Live {
//
//      override def formatMessage(msg: String): ZIO[Any, Nothing, String] =
//        ZIO.succeed(msg)
//    }
//  )
//
//  override def run(args: List[String]) =
//    localEnvironment >>> (for {
//      tcp       <- tcp.tcpTransport(10.seconds, 10.seconds)
//      localHost <- InetAddress.localHost.orDie
//      publicAddress <- SocketAddress
//                        .inetSocketAddress(localHost, 8010)
//                        .orDie
//      console <- ZIO.environment[Console]
//      handler = (channel: Connection) => {
//        for {
//          data <- channel.read
//          _    <- putStrLn(new String(data.toArray))
//          _    <- channel.send(data)
//        } yield ()
//      }.forever
//        .catchAll(ex => putStrLn("error: " + ex.msg))
//        .provide(console)
//
//      _ <- putStrLn("public address: " + publicAddress.toString())
//      _ <- bind(publicAddress)(handler)
//            .provide(tcp)
//            .use(ch => ZIO.never.ensuring(ch.close.ignore)).fork
//
//
//    } yield ()).ignore.as(0)
//}
//
//object TcpClient extends zio.App {
//  import zio.duration._
//  import zio.keeper.transport._
//
//  val localEnvironment = ZIO.environment[zio.ZEnv] @@ enrichWith[Logging[String]](
//    new Slf4jLogger.Live {
//
//      override def formatMessage(msg: String): ZIO[Any, Nothing, String] =
//        ZIO.succeed(msg)
//    }
//  )
//
//  override def run(args: List[String]) =
//    localEnvironment >>> (for {
//      tcp       <- tcp.tcpTransport(10.seconds, 10.seconds)
//      localHost <- InetAddress.localHost.orDie
//      publicAddress <- SocketAddress
//                        .inetSocketAddress(localHost, 8010)
//                        .orDie
//      _ <- putStrLn("connect to address: " + publicAddress.toString())
//      _ <- connect(publicAddress)
//            .provide(tcp)
//            .use(_.send(Chunk.fromArray("message from client".getBytes)).repeat(Schedule.recurs(100)))
//    } yield ()).ignore.as(0)
//}
