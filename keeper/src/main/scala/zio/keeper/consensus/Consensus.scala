package zio.keeper.consensus

import java.util.UUID

import zio._
import zio.clock.Clock
import zio.keeper.membership.{ ByteCodec, Membership, MembershipEvent, NodeId, TaggedCodec }
import zio.keeper.{ Error, Message, membership }
import zio.stream.{ Stream, ZSink }
import zio.keeper.protocol.InternalProtocol._
import upickle.default._
import zio.duration.Duration
import zio.logging.Logging

object Consensus {

  trait Service {
    def getLeader: UIO[NodeId]
    def onLeaderChange: Stream[Error, NodeId]
    def getView: UIO[List[NodeId]]
    def selfNode: UIO[NodeId]
    def receive: Stream[Error, Message]
    def broadcast(data: Chunk[Byte]): IO[Error, Unit]
    def send(data: Chunk[Byte], to: NodeId): IO[Error, Unit]
  }

}

/**
 *  Consensus live where leader is the oldest member in cluster
 */
object Coordinator {

  sealed private trait ConsensusMsg
  final private case object ConsensusViewRequest                     extends ConsensusMsg
  final private case class ConsensusViewResponse(view: List[NodeId]) extends ConsensusMsg
  final private case class ConsensusUserMsg(payload: Chunk[Byte])    extends ConsensusMsg
  private val viewRequestCodec  = ByteCodec.fromReadWriter(macroRW[ConsensusViewRequest.type])
  private val viewResponseCodec = ByteCodec.fromReadWriter(macroRW[ConsensusViewResponse])

  private val userMsgCodec = ByteCodec.fromReadWriter(
    implicitly[ReadWriter[Array[Byte]]]
      .bimap[ConsensusUserMsg](_.payload.toArray, bA => ConsensusUserMsg(Chunk.fromArray(bA)))
  )

  implicit private val consensusCodec: TaggedCodec[ConsensusMsg] =
    TaggedCodec.instance[ConsensusMsg](
      {
        case _: ConsensusViewRequest.type => 50
        case _: ConsensusViewResponse     => 51
        case _: ConsensusUserMsg          => 52
      }, {
        case 50 => viewRequestCodec.asInstanceOf[ByteCodec[ConsensusMsg]]
        case 51 => viewResponseCodec.asInstanceOf[ByteCodec[ConsensusMsg]]
        case 52 => userMsgCodec.asInstanceOf[ByteCodec[ConsensusMsg]]
      }
    )

  private def runEventLoop(ref: Ref[List[NodeId]], self: NodeId) =
    membership.events.foreach {
      case MembershipEvent.Join(member) =>
        ref.update(view => view :+ member.nodeId)

      case MembershipEvent.Leave(member) =>
        for {
          successor <- ref.modify { view =>
                        val idx       = view.indexOf(member.nodeId)
                        val successor = if (idx == 0) view.tail.headOption else None
                        (successor, view.filterNot(_ == member.nodeId))
                      }
          _ <- successor match {
                case Some(id) if id == self =>
                  for {
                    view    <- ref.get
                    payload <- TaggedCodec.write[ConsensusMsg](ConsensusViewResponse(view))
                    _       <- membership.broadcast(payload)
                  } yield ()
                case None =>
                  UIO.unit
              }
        } yield ()

      case MembershipEvent.Unreachable(_) =>
        UIO.unit
    }

  private def runMsgLoop(self: NodeId, ref: Ref[List[NodeId]], queue: Queue[Message]) =
    membership.receive.tap { msg =>
      for {
        read <- TaggedCodec.read[ConsensusMsg](msg.payload)
        _ <- read match {
              case ConsensusViewRequest =>
                for {
                  _      <- logging.logInfo(s"[CONSENSUS] Received view request")
                  view   <- ref.get
                  leader = view.headOption
                  _ <- leader match {
                        case Some(value) if value == self =>
                          for {
                            payload <- TaggedCodec.write[ConsensusMsg](ConsensusViewResponse(view))
                            _       <- membership.send(payload, msg.sender)
                            _       <- logging.logInfo(s"[CONSENSUS] View request answered")
                          } yield ()
                        case _ => UIO.unit
                      }
                } yield ()
              case ConsensusViewResponse(view) =>
                logging.logInfo(s"[CONSENSUS] Received view response") *> ref.set(view)
              case ConsensusUserMsg(payload) =>
                logging.logInfo("[CONSENSUS] Received user msg") *> queue.offer(
                  Message(UUID.randomUUID(), msg.sender, payload)
                )
            }
      } yield ()
    }.runDrain

  private def setCurrentView(ref: Ref[List[NodeId]]) =
    for {
      mem     <- ZIO.access[Membership](_.get[Membership.Service])
      clock   <- ZIO.access[Clock](_.get[Clock.Service])
      payload <- TaggedCodec.write[ConsensusMsg](ConsensusViewRequest)
      _       <- logging.logInfo(s"[CONSENSUS] Asking for a view")
      _       <- mem.broadcast(payload)
      response = mem.receive
        .mapM(msg => TaggedCodec.read[ConsensusMsg](msg.payload))
        .collect { case s: ConsensusViewResponse => s }
        .run(ZSink.head[ConsensusViewResponse])
        .flatMap {
          case None => UIO(false)
          case Some(s) =>
            logging.logInfo(s"[CONSENSUS] Received view: ${s.view}") *> ref.set(s.view).map(_ => true)
        }
      timeout = clock.sleep(Duration.fromNanos(3000000000L)) *> UIO(false)
      result  <- response.race(timeout)
    } yield result

  def create(): ZLayer[Membership with Clock with Logging, Error, Consensus] =
    ZLayer.fromEffect {
      for {
        queue       <- Queue.unbounded[Message]
        selfId      <- membership.localMember
        _           <- logging.logInfo(s"[CONSENSUS] Starting consensus layer for node: ${selfId.nodeId}")
        members     <- membership.nodes
        _           <- logging.logInfo(s"[CONSENSUS] Present nodes: $members")
        consMembers <- Ref.make(List(selfId.nodeId))
        _           <- if (members.nonEmpty) setCurrentView(consMembers).doUntilEquals(true) else UIO.unit
        memHas      <- ZIO.environment[Membership]
        _           <- runEventLoop(consMembers, selfId.nodeId).fork
        _           <- runMsgLoop(selfId.nodeId, consMembers, queue).fork
        _           <- logging.logInfo("[CONSENSUS] Consensus is running")
      } yield new Consensus.Service {
        def getLeader: UIO[NodeId] =
          consMembers.get.map(_.headOption.getOrElse(selfId.nodeId))
        def getView: UIO[List[NodeId]]      = consMembers.get
        def receive: Stream[Error, Message] = Stream.fromQueue(queue)
        def broadcast(data: Chunk[Byte]): IO[Error, Unit] =
          for {
            payload <- TaggedCodec.write[ConsensusMsg](ConsensusUserMsg(data))
            _       <- membership.broadcast(payload).provide(memHas)
          } yield ()
        def send(data: Chunk[Byte], to: NodeId): IO[Error, Unit] =
          for {
            payload <- TaggedCodec.write[ConsensusMsg](ConsensusUserMsg(data))
            _       <- membership.send(payload, to).provide(memHas)
          } yield ()
        def onLeaderChange: Stream[Error, NodeId] = ???
        def selfNode: UIO[NodeId]                 = UIO(selfId.nodeId)
      }

    }

}
