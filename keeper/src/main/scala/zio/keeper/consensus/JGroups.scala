package zio.keeper.consensus

import zio.{ Chunk, Has, IO, Tagged, UIO, ZIO, ZLayer, clock, logging }
import zio.clock.Clock
import zio.duration._
import zio.keeper.Error
import zio.keeper.membership.{ ByteCodec, TaggedCodec }
import zio.stream.ZSink
import upickle.default._
import zio.logging.Logging

/**
 *  Implemented by user
 */
object ReceiverAdapter {

  trait Service {
    def receive(data: Chunk[Byte]): IO[Error, Unit]

    def setState(data: Chunk[Byte]): IO[Error, Unit]

    def getState: IO[Error, Chunk[Byte]]
  }

}

object JGroups {

  sealed trait JGroupsMsg
  object JGroupsMsg {
    final case object StateRequest                       extends JGroupsMsg
    final case class StateResponse(payload: Chunk[Byte]) extends JGroupsMsg
    final case class UserMessage(payload: Chunk[Byte])   extends JGroupsMsg
  }

  import JGroupsMsg._

  private val stateRequestCodec = ByteCodec.fromReadWriter(
    implicitly[ReadWriter[Array[Byte]]].bimap[StateRequest.type](_ => Array(1, 1), _ => StateRequest)
  )

  private val StateResponseCodec = ByteCodec.fromReadWriter(
    implicitly[ReadWriter[Array[Byte]]]
      .bimap[StateResponse](_.payload.toArray, bA => StateResponse(Chunk.fromArray(bA)))
  )

  private val UserMessageCodec = ByteCodec.fromReadWriter(
    implicitly[ReadWriter[Array[Byte]]].bimap[UserMessage](_.payload.toArray, bA => UserMessage(Chunk.fromArray(bA)))
  )

  implicit private val protocolCodec: TaggedCodec[JGroupsMsg] =
    TaggedCodec.instance[JGroupsMsg](
      {
        case _: StateRequest.type => 21
        case _: StateResponse     => 22
        case _: UserMessage       => 23
      }, {
        case 21 => stateRequestCodec.asInstanceOf[ByteCodec[JGroupsMsg]]
        case 22 => StateResponseCodec.asInstanceOf[ByteCodec[JGroupsMsg]]
        case 23 => UserMessageCodec.asInstanceOf[ByteCodec[JGroupsMsg]]
      }
    )

  trait Service {
    def send(data: Chunk[Byte]): IO[Error, Unit]
  }

  val live: ZLayer[Consensus, Error, JGroups] =
    ZLayer.fromService[Consensus.Service, JGroups.Service] { c =>
      new Service {
        override def send(data: Chunk[Byte]): IO[Error, Unit] =
          TaggedCodec.write[JGroupsMsg](UserMessage(data)).flatMap(c.broadcast)
      }
    }

  private val msgLoop: ZIO[Consensus with ReceiverAdapter with Logging, Error, Unit] =
    for {
      env    <- ZIO.environment[Consensus with ReceiverAdapter with Logging]
      c      = env.get[Consensus.Service]
      ra     = env.get[ReceiverAdapter.Service]
      selfId <- c.selfNode
      _ <- c.receive.tap { rawMsg =>
            TaggedCodec.read[JGroupsMsg](rawMsg.payload).flatMap {
              case StateRequest =>
                for {
                  _ <- logging.logInfo(s"[JGROUPS] Received state request")
                  _ <- ZIO.whenM[Logging, Error](c.getLeader.map(_ == selfId)) {
                        for {
                          state   <- ra.getState
                          payload <- TaggedCodec.write[JGroupsMsg](StateResponse(state))
                          _       <- c.send(payload, rawMsg.sender)
                          _       <- logging.logInfo(s"[JGROUPS] Answering state request")
                        } yield ()
                      }
                } yield ()
              case StateResponse(payload) =>
                for {
                  _      <- logging.logInfo(s"[JGROUPS] Received state response")
                  leader <- c.getLeader
                  _      <- IO.when(leader == rawMsg.sender)(ra.setState(payload))
                } yield ()
              case UserMessage(payload) =>
                logging.logInfo(s"[JGROUPS] Received user message") *> ra.receive(payload)
            }
          }.runDrain
    } yield ()

  private val initializeState: ZIO[Clock with Logging with ReceiverAdapter with Consensus, Error, Boolean] =
    for {
      c       <- ZIO.access[Consensus](_.get[Consensus.Service])
      ra      <- ZIO.access[ReceiverAdapter](_.get[ReceiverAdapter.Service])
      leader  <- c.getLeader
      payload <- TaggedCodec.write[JGroupsMsg](StateRequest)
      _       <- logging.logInfo(s"[JGROUPS] Asking for a state")
      _       <- c.send(payload, leader)
      response = c.receive
        .filter(_.sender == leader)
        .mapM(msg => TaggedCodec.read[JGroupsMsg](msg.payload))
        .collect { case s: StateResponse => s }
        .run(ZSink.head[StateResponse])
        .flatMap {
          case None    => UIO(false)
          case Some(s) => ra.setState(s.payload).as(true)
        }
      timeout = clock.sleep(3.seconds) *> UIO(false)
      result  <- response.race(timeout)
    } yield result

  def create[A: Tagged]: ZLayer[Consensus with ReceiverAdapter with Clock with Logging with Has[A], Error, Has[A]] =
    ZLayer.fromEffect {
      for {
        _   <- clock.sleep(3.seconds)
        env <- ZIO.environment[Consensus with ReceiverAdapter with Logging with Has[A]]
        t   <- ZIO.access[Has[A]](_.get[A])
        c   = env.get[Consensus.Service]
        v   <- c.getView
        _   <- ZIO.when(v.size > 1)(initializeState.doUntilEquals(true))
        _   <- msgLoop.provide(env).fork
        _   <- logging.logInfo(s"[JGROUPS] JGroups is running")
      } yield t
    }

}
