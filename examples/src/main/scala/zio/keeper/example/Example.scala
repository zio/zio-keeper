package zio.keeper.example

import upickle.default._
import zio._
import zio.clock._
import zio.config.Config
import zio.console._
import zio.duration._
import zio.keeper.ByteCodec
import zio.keeper.discovery.Discovery
import zio.keeper.example.TestNode.UserProtocolExample.{ Ping, Pong }
import zio.keeper.ByteCodec
import zio.keeper.membership.swim.{ SWIM, SwimConfig }
import zio.logging.Logging
import zio.nio.core.{ InetAddress, SocketAddress }
import zio.keeper.membership._
import zio.logging._

object Node1 extends zio.App {

  def run(args: List[String]) =
    TestNode.run(5557, Set.empty)
}

object Node2 extends zio.App {

  def run(args: List[String]) =
    TestNode.run(5558, Set(5557))
}

object Node3 extends zio.App {

  def run(args: List[String]) =
    TestNode.run(5559, Set(5557))
}

object TestNode {

  val logging = Logging.console((_, msg) => msg)

  def discovery(others: Set[Int]): ULayer[Discovery] =
    ZLayer.fromManaged(
      ZManaged
        .foreach(others) { port =>
          InetAddress.localHost.flatMap(SocketAddress.inetSocketAddress(_, port)).toManaged_
        }
        .orDie
        .flatMap(addrs => Discovery.staticList(addrs.toSet).build.map(_.get))
    )

  def dependencies(port: Int, others: Set[Int]) = {
    val config     = Config.fromMap(Map("PORT" -> port.toString), SwimConfig.description).orDie
    val seeds      = discovery(others)
    val membership = (seeds ++ logging ++ Clock.live ++ config) >>> SWIM.live[UserProtocolExample]
    logging ++ membership
  }

  sealed trait UserProtocolExample

  object UserProtocolExample {
    final case class Ping(i: Int) extends UserProtocolExample
    final case class Pong(i: Int) extends UserProtocolExample

    implicit val pingCodec: ByteCodec[Ping] =
      ByteCodec.fromReadWriter(macroRW[Ping])

    implicit val pongCodec: ByteCodec[Pong] =
      ByteCodec.fromReadWriter(macroRW[Pong])

    implicit val codec: ByteCodec[UserProtocolExample] =
      ByteCodec.tagged[UserProtocolExample][
        Ping,
        Pong
      ]

  }

  def run(port: Int, otherPorts: Set[Int]) =
    program
      .provideCustomLayer(dependencies(port, otherPorts))
      .catchAll(ex => putStrLn("error: " + ex).as(1))

  val program =
//   Fiber.dumpAll.flatMap(ZIO.foreach(_)(_.prettyPrintM.flatMap(putStrLn(_).provideLayer(ZEnv.live)))).delay(10.seconds).uninterruptible.fork.toManaged_ *>
    for {
      _ <- sleep(5.seconds)
      _ <- events[UserProtocolExample].foreach(event => log.info("membership event: " + event)).fork
      _ <- broadcast[UserProtocolExample](Ping(1))
      _ <- receive[UserProtocolExample].foreach {
            case (sender, message) =>
              log.info(s"receive message: $message from: $sender") *>
                ZIO.whenCase(message) {
                  case Ping(i) => send[UserProtocolExample](Pong(i + 1), sender).ignore
                  case Pong(i) => send[UserProtocolExample](Pong(i + 1), sender).ignore
                }
          }
    } yield 0

}
