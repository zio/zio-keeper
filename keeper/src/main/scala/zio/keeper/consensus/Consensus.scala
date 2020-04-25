package zio.keeper.consensus

import upickle.default._

import zio._
import zio.clock.Clock
import zio.duration._
import zio.keeper.membership.swim.Nodes.NodeState
import zio.keeper.membership.{ ByteCodec, Membership, MembershipEvent, NodeAddress, TaggedCodec }
import zio.keeper.{ Error, membership }
import zio.stream.{ Stream, ZSink }
import zio.logging.Logging.Logging

object Consensus {

  trait Service {
    def getLeader: UIO[NodeAddress]
    def onLeaderChange: Stream[Error, NodeAddress]
    def getView: UIO[Vector[NodeAddress]]
    def selfNode: UIO[NodeAddress]
    def receive: Stream[Error, (NodeAddress, Chunk[Byte])]
    def broadcast(data: Chunk[Byte]): IO[Error, Unit]
    def send(data: Chunk[Byte], to: NodeAddress): IO[Error, Unit]
  }

}

/**
 *  Consensus live where leader is the oldest member in cluster
 */
object Coordinator {

  sealed trait ConsensusMsg

  object ConsensusMsg {
    final case object ConsensusViewRequest                            extends ConsensusMsg
    final case class ConsensusViewResponse(view: Vector[NodeAddress]) extends ConsensusMsg
    final case class ConsensusUserMsg(payload: Chunk[Byte])           extends ConsensusMsg
  }

  import ConsensusMsg._

  type Message = (NodeAddress, Chunk[Byte])

  private val viewRequestCodec  = ByteCodec.fromReadWriter(macroRW[ConsensusViewRequest.type])
  private val viewResponseCodec = ByteCodec.fromReadWriter(macroRW[ConsensusViewResponse])

  private val userMsgCodec = ByteCodec.fromReadWriter(
    implicitly[ReadWriter[Array[Byte]]]
      .bimap[ConsensusUserMsg](_.payload.toArray, bA => ConsensusUserMsg(Chunk.fromArray(bA)))
  )

  implicit val consensusCodec: TaggedCodec[ConsensusMsg] =
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

  private def runEventLoop(ref: Ref[Vector[NodeAddress]], self: NodeAddress) =
    membership.events[ConsensusMsg].foreach {
      case MembershipEvent.Join(member) =>
        ref.update(view => view :+ member)

      case MembershipEvent.NodeStateChanged(member, _, NodeState.Left) =>
        for {
          successor <- ref.modify { view =>
                        val idx       = view.indexOf(member)
                        val successor = if (idx == 0) view.tail.headOption else None
                        (successor, view.filterNot(_ == member))
                      }
          _ <- successor match {
                case Some(id) if id == self =>
                  for {
                    view <- ref.get
                    _    <- ZIO.foreach(view)(n => membership.send[ConsensusMsg](ConsensusViewResponse(view), n))
                  } yield ()
                case _ =>
                  UIO.unit
              }
        } yield ()

      case _: MembershipEvent =>
        UIO.unit
    }

  private def runMsgLoop(self: NodeAddress, ref: Ref[Vector[NodeAddress]], queue: Queue[Message]) =
    membership
      .receive[ConsensusMsg]
    .mapM(s => logging.log.info(s"[CONSENSUS] received: $s") *> UIO(s))
      .tap {
        case (sender, msg) =>
          msg match {
            case ConsensusViewRequest =>
              for {
                _      <- logging.log.info(s"[CONSENSUS] Received view request")
                view   <- ref.get
                leader = view.headOption
                _ <- leader match {
                      case Some(value) if value == self =>
                        for {
                          _ <- membership.send[ConsensusMsg](ConsensusViewResponse(view), sender)
                          _ <- logging.log.info(s"[CONSENSUS] View request answered")
                        } yield ()
                      case _ => UIO.unit
                    }
              } yield ()
            case ConsensusViewResponse(view) =>
              logging.log.info(s"[CONSENSUS] Received view response") *> ref.set(view)
            case ConsensusUserMsg(payload) =>
              logging.log.info("[CONSENSUS] Received user msg") *> queue.offer((sender, payload))
          }
      }
      .runDrain

  private def setCurrentView(ref: Ref[Vector[NodeAddress]]) =
    for {
      mem   <- ZIO.access[Membership[ConsensusMsg]](_.get[Membership.Service[ConsensusMsg]])
      clock <- ZIO.access[Clock](_.get[Clock.Service])
      _     <- logging.log.info(s"[CONSENSUS] Asking for a view")
      nodes <- mem.nodes
      _     <- ZIO.foreach(nodes)(n => mem.send(ConsensusViewRequest, n))
      response = mem.receive
        .collect { case (_, s: ConsensusViewResponse) => s }
        .run(ZSink.head[ConsensusViewResponse])
        .flatMap {
          case None => UIO(false)
          case Some(s) =>
            logging.log.info(s"[CONSENSUS] Received view: ${s.view}") *> ref.set(s.view).as(true)
        }
      timeout = clock.sleep(3.seconds) *> UIO(false)
      result  <- response.race(timeout)
    } yield result

  val live: ZLayer[Membership[ConsensusMsg] with Clock with Logging, Error, Consensus] =
    ZLayer.fromEffect {
      for {
        _           <- clock.sleep(7.seconds)
        queue       <- Queue.unbounded[Message]
        selfId      <- membership.localMember[ConsensusMsg]
        _           <- logging.log.info(s"[CONSENSUS] Starting consensus layer for node: selfId")
        members     <- membership.nodes[ConsensusMsg]
        _           <- logging.log.info(s"[CONSENSUS] Present nodes: $members")
        consMembers <- Ref.make(Vector(selfId))
        _           <- ZIO.when(members.nonEmpty)(setCurrentView(consMembers).doUntilEquals(true))
        memHas      <- ZIO.environment[Membership[ConsensusMsg]]
        _           <- runEventLoop(consMembers, selfId).fork
        _           <- runMsgLoop(selfId, consMembers, queue).fork
        _           <- logging.log.info("[CONSENSUS] Consensus is running")
      } yield new Consensus.Service {
        def getLeader: UIO[NodeAddress] =
          consMembers.get.map(_.headOption.getOrElse(selfId))
        def getView: UIO[Vector[NodeAddress]] = consMembers.get
        def receive: Stream[Error, Message]   = Stream.fromQueue(queue)
        def broadcast(data: Chunk[Byte]): IO[Error, Unit] =
          (for {
            nodes <- membership.nodes[ConsensusMsg]
            _ <- ZIO.foreach(nodes)(n => membership.send[ConsensusMsg](ConsensusUserMsg(data), n))
          } yield ()).provide(memHas)
        def send(data: Chunk[Byte], to: NodeAddress): IO[Error, Unit] =
          membership.send[ConsensusMsg](ConsensusUserMsg(data), to).provide(memHas)
        def onLeaderChange: Stream[Error, NodeAddress] = ???
        def selfNode: UIO[NodeAddress]                 = UIO(selfId)
      }

    }

}
