package zio.keeper.consensus

import zio.{ Chunk, Has, IO, Tagged, UIO, ZIO, ZLayer, clock, logging }
import zio.clock.Clock
import zio.duration.Duration
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

  sealed private trait JGroupsProtocol
  private case object StateRequest                       extends JGroupsProtocol
  private case class StateResponse(payload: Chunk[Byte]) extends JGroupsProtocol
  private case class UserMessage(payload: Chunk[Byte])   extends JGroupsProtocol

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

  implicit private val protocolCodec: TaggedCodec[JGroupsProtocol] =
    TaggedCodec.instance[JGroupsProtocol](
      {
        case _: StateRequest.type => 21
        case _: StateResponse     => 22
        case _: UserMessage       => 23
      }, {
        case 21 => stateRequestCodec.asInstanceOf[ByteCodec[JGroupsProtocol]]
        case 22 => StateResponseCodec.asInstanceOf[ByteCodec[JGroupsProtocol]]
        case 23 => UserMessageCodec.asInstanceOf[ByteCodec[JGroupsProtocol]]
      }
    )

  trait Service {
    def send(data: Chunk[Byte]): IO[Error, Unit]
  }

  val live: ZLayer[Consensus, Error, JGroups] =
    ZLayer.fromService[Consensus.Service, JGroups.Service] { c =>
      new Service {
        override def send(data: Chunk[Byte]): IO[Error, Unit] =
          for {
            payload <- TaggedCodec.write[JGroupsProtocol](UserMessage(data))
            _       <- c.broadcast(payload)
          } yield ()
      }
    }

  private val msgLoop: ZIO[Consensus with ReceiverAdapter with Logging, Error, Unit] =
    for {
      c      <- ZIO.access[Consensus](_.get[Consensus.Service])
      ra     <- ZIO.access[ReceiverAdapter](_.get[ReceiverAdapter.Service])
      selfId <- c.selfNode
      _ <- c.receive.tap { rawMsg =>
            for {
              msg <- TaggedCodec.read[JGroupsProtocol](rawMsg.payload)
              _ <- msg match {
                    case StateRequest =>
                      for {
                        _      <- logging.logInfo(s"[JGROUPS] Received state request")
                        leader <- c.getLeader
                        _ <- if (leader == selfId) {
                              for {
                                state   <- ra.getState
                                payload <- TaggedCodec.write[JGroupsProtocol](StateResponse(state))
                                _       <- c.send(payload, rawMsg.sender)
                                _       <- logging.logInfo(s"[JGROUPS] Answering state request")
                              } yield ()
                            } else UIO.unit
                      } yield ()
                    case StateResponse(payload) =>
                      for {
                        _      <- logging.logInfo(s"[JGROUPS] Received state response")
                        leader <- c.getLeader
                        _ <- if (leader == rawMsg.sender) {
                              ra.setState(payload)
                            } else UIO.unit
                      } yield ()
                    case UserMessage(payload) =>
                      logging.logInfo(s"[JGROUPS] Received user message") *> ra.receive(payload)
                  }
            } yield ()
          }.runDrain
    } yield ()

  private val initializeState: ZIO[Clock with Logging with ReceiverAdapter with Consensus, Error, Boolean] =
    for {
      c       <- ZIO.access[Consensus](_.get[Consensus.Service])
      ra      <- ZIO.access[ReceiverAdapter](_.get[ReceiverAdapter.Service])
      leader  <- c.getLeader
      payload <- TaggedCodec.write[JGroupsProtocol](StateRequest)
      _       <- logging.logInfo(s"[JGROUPS] Asking for a state")
      _       <- c.send(payload, leader)
      response = c.receive
        .filter(_.sender == leader)
        .mapM(msg => TaggedCodec.read[JGroupsProtocol](msg.payload))
        .collect { case s: StateResponse => s }
        .run(ZSink.head[StateResponse])
        .flatMap {
          case None    => UIO(false)
          case Some(s) => ra.setState(s.payload).map(_ => true)
        }
      timeout = clock.sleep(Duration.fromNanos(3000000000L)) *> UIO(false)
      result  <- response.race(timeout)
    } yield result

  def create[T: Tagged](): ZLayer[Consensus with ReceiverAdapter with Clock with Logging with Has[T], Error, Has[T]] =
    ZLayer.fromEffect {
      for {
        _   <- clock.sleep(Duration.fromNanos(3000000000L))
        env <- ZIO.environment[Consensus with ReceiverAdapter with Logging with Has[T]]
        t   <- ZIO.access[Has[T]](_.get[T])
        c   = env.get[Consensus.Service]
        v   <- c.getView
        _   <- if (v.size > 1) initializeState.doUntilEquals(true) else UIO.unit
        _   <- msgLoop.provide(env).fork
        _   <- logging.logInfo(s"[JGROUPS] JGroups is running")
      } yield t
    }

}
