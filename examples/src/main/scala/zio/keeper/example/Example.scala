package zio.keeper.example

import upickle.default._
import zio._
import zio.clock._
import zio.config.Config
import zio.console._
import zio.duration._
import zio.keeper.ByteCodec
import zio.keeper.discovery.Discovery
import zio.keeper.example.TestNode.PingPong.{ Ping, Pong }
import zio.keeper.TaggedCodec
import zio.keeper.membership.swim.{ SWIM, SwimConfig }
import zio.logging.Logging
import zio.nio.core.{ InetAddress, SocketAddress }

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
    TestNode.start(5559, Set(5557))
}

object TestNode {

  val logging = Logging.console((_, msg) => msg)

  sealed trait PingPong

  object PingPong {
    case class Ping(i: Int) extends PingPong
    case class Pong(i: Int) extends PingPong

    implicit val pingCodec: ByteCodec[Ping] =
      ByteCodec.fromReadWriter(macroRW[Ping])

    implicit val pongCodec: ByteCodec[Pong] =
      ByteCodec.fromReadWriter(macroRW[Pong])

    implicit def tagged(implicit p1: ByteCodec[Ping], p2: ByteCodec[Pong]) =
      TaggedCodec.instance[PingPong]({
        case Ping(_) => 1
        case Pong(_) => 2
      }, {
        case 1 => p1.asInstanceOf[ByteCodec[PingPong]]
        case 2 => p2.asInstanceOf[ByteCodec[PingPong]]
      })
  }

  def start(port: Int, otherPorts: Set[Int]) =
//   Fiber.dumpAll.flatMap(ZIO.foreach(_)(_.prettyPrintM.flatMap(putStrLn(_).provideLayer(ZEnv.live)))).delay(10.seconds).uninterruptible.fork.toManaged_ *>
    environment(port, otherPorts).orDie.flatMap(
      env =>
        (for {
          membership0 <- ZManaged.access[SWIM[PingPong]](_.get)
          _           <- sleep(5.seconds).toManaged_
          nodes       <- membership0.nodes.toManaged_
          _           <- ZIO.foreach(nodes)(n => membership0.send(Ping(1), n)).toManaged_
          _ <- membership0.receive.foreach {
                case (sender, message) =>
                  putStrLn("receive message: " + message) *> membership0.send(Pong(1), sender).ignore *> sleep(
                    5.seconds
                  )
              }.toManaged_
        } yield 0)
          .provideCustomLayer(env)
          .catchAll(ex => putStrLn("error: " + ex).toManaged_.as(1))
    )

  private def environment(port: Int, others: Set[Int]) =
    discovery(others).map { dsc =>
      val config = Config.fromMap(Map("PORT" -> port.toString), SwimConfig.description)
      val mem    = (dsc ++ logging ++ Clock.live ++ config) >>> SWIM.live[PingPong]
      dsc ++ logging ++ mem
    }

  def discovery(others: Set[Int]): Managed[Exception, Layer[Nothing, Discovery]] =
    ZManaged
      .foreach(others) { port =>
        InetAddress.localHost.flatMap(SocketAddress.inetSocketAddress(_, port)).toManaged_
      }
      .orDie
      .map(addrs => Discovery.staticList(addrs.toSet))

}
