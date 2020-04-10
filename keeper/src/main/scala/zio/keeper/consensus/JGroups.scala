package zio.keeper.consensus

import zio.{Chunk, Has, IO, Tagged, UIO, ZIO, ZLayer, clock, console}
import zio.clock.Clock
import zio.duration.Duration
import zio.keeper.Error
import zio.keeper.membership.{ByteCodec, TaggedCodec}
import zio.stream.ZSink
import upickle.default._

sealed trait JGroupsProtocol
case class StateRequest() extends JGroupsProtocol
case class StateResponse(payload: Chunk[Byte]) extends JGroupsProtocol
case class UserMessage(payload: Chunk[Byte]) extends JGroupsProtocol

object JGroups {

  val q1 = ByteCodec.fromReadWriter(
    implicitly[ReadWriter[Array[Byte]]].bimap[StateRequest](_ => Array.empty, _ => StateRequest())
  )
  val q2 = ByteCodec.fromReadWriter(
    implicitly[ReadWriter[Array[Byte]]].bimap[StateResponse](_.payload.toArray, bA => StateResponse(Chunk.fromArray(bA)))
  )
  val q3 = ByteCodec.fromReadWriter(
    implicitly[ReadWriter[Array[Byte]]].bimap[UserMessage](_.payload.toArray, bA => UserMessage(Chunk.fromArray(bA)))
  )
  implicit val xddd: TaggedCodec[JGroupsProtocol] =
    TaggedCodec.instance[JGroupsProtocol](
      {
        case _: StateRequest => 21
        case _: StateResponse => 22
        case _: UserMessage => 23
      }, {
        case 21 => q1.asInstanceOf[ByteCodec[JGroupsProtocol]]
        case 22 => q2.asInstanceOf[ByteCodec[JGroupsProtocol]]
        case 23 => q3.asInstanceOf[ByteCodec[JGroupsProtocol]]
      }
    )

  trait Service {
    def send(payload: Chunk[Byte]): IO[Error, Unit]
  }

  val live: ZLayer[Consensus, Error, JGroups] =
    ZLayer.fromService[Consensus.Service, JGroups.Service] { c =>
      new Service {
        override def send(payload: Chunk[Byte]): IO[Error, Unit] =
          for {
            toSend <- TaggedCodec.write[JGroupsProtocol](UserMessage(payload))
            _ <- c.broadcast(toSend)
          } yield ()
      }
    }

  private def msgLoop: ZIO[Consensus with ReceiverAdapter, Error, Unit] =
    for {
      c <- ZIO.access[Consensus](_.get[Consensus.Service])
      ra <- ZIO.access[ReceiverAdapter](_.get[ReceiverAdapter.Service])
      selfId <- c.selfNode
      _ <- c.receive.tap { rawMsg =>
        for {
          msg <- TaggedCodec.read[JGroupsProtocol](rawMsg.payload)
          _ <- msg match {
            case StateRequest() =>
              for {
                _ <- console.putStrLn("State request rec").provideLayer(console.Console.live)
                leader <- c.getLeader
                _ <- console.putStrLn(leader.toString + " " + selfId.toString).provideLayer(console.Console.live)
                _ <- if (leader == selfId) {
                  for {
                    st <- ra.getState
                    payload <- TaggedCodec.write[JGroupsProtocol](StateResponse(st))
                    _ <- c.send(payload, rawMsg.sender)
                  } yield ()
                } else {
                  UIO.unit
                }
              } yield ()
            case StateResponse(payload) =>
              for {
                _ <- console.putStrLn("State response received").provideLayer(console.Console.live)
                leader <- c.getLeader
                _ <- if (leader == rawMsg.sender) {
                  ra.setState(payload)
                } else {
                  UIO.unit
                }
              } yield ()
            case UserMessage(payload) =>
              console.putStrLn("user msg received").provideLayer(console.Console.live) *>
              ra.receive(payload)
          }
        } yield ()

      }.runDrain
    } yield ()

  def create[T: Tagged]()
  : ZLayer[Consensus with ReceiverAdapter with Clock with Has[T], Error, Has[T]] =
    ZLayer.fromEffect {
      for {
        _ <- clock.sleep(Duration.fromNanos(3000000000L))
        env   <- ZIO.environment[Consensus with ReceiverAdapter with Has[T]]
        clock <- ZIO.access[Clock](_.get[Clock.Service])
        ra    <- ZIO.access[ReceiverAdapter](_.get[ReceiverAdapter.Service])
        t     <- ZIO.access[Has[T]](_.get[T])
        c      = env.get[Consensus.Service]
        eff = for {
          leader <- c.getLeader
          payload <- TaggedCodec.write[JGroupsProtocol](StateRequest())
          _ <- c.send(payload, leader)
          eee = c.receive
            .filter(_.sender == leader)
            .mapM(mm => TaggedCodec.read[JGroupsProtocol](mm.payload))
            .collect{case s: StateResponse => s}
            .run(ZSink.head[StateResponse])
            .flatMap {
              case None    => UIO(false)
              case Some(s) => ra.setState(s.payload).map(_ => true)
            }
          www = clock.sleep(Duration.fromNanos(3000000000L)) *> UIO(false)
          result <- eee.race(www)
        } yield result
        v <- c.getView
        _ <- if (v.size > 1) eff.doUntilEquals(true) else UIO.unit
        _ <- msgLoop.provide(env).fork
        _ <- console.putStrLn("JGroups is running").provideLayer(console.Console.live)
      } yield t
    }

}