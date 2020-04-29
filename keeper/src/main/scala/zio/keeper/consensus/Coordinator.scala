package zio.keeper.consensus

import upickle.default._

import zio.clock.Clock
import zio.duration._
import zio.keeper.{ Error, membership }
import zio.keeper.membership.{ ByteCodec, Membership, MembershipEvent, NodeAddress, TaggedCodec }
import zio.keeper.membership.swim.Nodes.NodeState
import zio._
import zio.logging.Logging.Logging
import zio.stream.{ Stream, ZSink }

/**
 *  Consensus implementation - where leader is the oldest member in the cluster
 */
object Coordinator {

  sealed trait CoordinatorMsg

  object CoordinatorMsg {
    final case object CoordinatorViewRequest$                           extends CoordinatorMsg
    final case class CoordinatorViewResponse(view: Vector[NodeAddress]) extends CoordinatorMsg
    final case class CoordinatorUserMsg(payload: Chunk[Byte])           extends CoordinatorMsg
  }

  import CoordinatorMsg._

  type Message = (NodeAddress, Chunk[Byte])

  private val viewRequestCodec  = ByteCodec.fromReadWriter(macroRW[CoordinatorViewRequest$.type])
  private val viewResponseCodec = ByteCodec.fromReadWriter(macroRW[CoordinatorViewResponse])

  private val userMsgCodec = ByteCodec.fromReadWriter(
    implicitly[ReadWriter[Array[Byte]]]
      .bimap[CoordinatorUserMsg](_.payload.toArray, bA => CoordinatorUserMsg(Chunk.fromArray(bA)))
  )

  implicit val coordinatorCodec: TaggedCodec[CoordinatorMsg] =
    TaggedCodec.instance[CoordinatorMsg](
      {
        case _: CoordinatorViewRequest$.type => 50
        case _: CoordinatorViewResponse      => 51
        case _: CoordinatorUserMsg           => 52
      }, {
        case 50 => viewRequestCodec.asInstanceOf[ByteCodec[CoordinatorMsg]]
        case 51 => viewResponseCodec.asInstanceOf[ByteCodec[CoordinatorMsg]]
        case 52 => userMsgCodec.asInstanceOf[ByteCodec[CoordinatorMsg]]
      }
    )

  private def runEventLoop(ref: Ref[Vector[NodeAddress]], self: NodeAddress) =
    membership.events[CoordinatorMsg].foreach {
      case MembershipEvent.Join(member) =>
        ref.update(view => view :+ member)

      case MembershipEvent.NodeStateChanged(member, _, NodeState.Death) =>
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
                    _    <- ZIO.foreach(view)(n => membership.send[CoordinatorMsg](CoordinatorViewResponse(view), n))
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
      .receive[CoordinatorMsg]
      .mapM(s => logging.log.info(s"[CONSENSUS] received: $s") *> UIO(s))
      .tap {
        case (sender, msg) =>
          msg match {
            case CoordinatorViewRequest$ =>
              for {
                _      <- logging.log.info("[CONSENSUS] Received view request")
                view   <- ref.get
                leader = view.headOption
                _ <- leader match {
                      case Some(value) if value == self =>
                        for {
                          _ <- membership.send[CoordinatorMsg](CoordinatorViewResponse(view), sender)
                          _ <- logging.log.info("[CONSENSUS] View request answered")
                        } yield ()
                      case _ => UIO.unit
                    }
              } yield ()
            case CoordinatorViewResponse(view) =>
              logging.log.info("[CONSENSUS] Received view response") *> ref.set(view)
            case CoordinatorUserMsg(payload) =>
              logging.log.info("[CONSENSUS] Received user msg") *> queue.offer((sender, payload))
          }
      }
      .runDrain

  private def setCurrentView(ref: Ref[Vector[NodeAddress]]) =
    for {
      mem   <- ZIO.access[Membership[CoordinatorMsg]](_.get[Membership.Service[CoordinatorMsg]])
      clock <- ZIO.access[Clock](_.get[Clock.Service])
      _     <- logging.log.info("[CONSENSUS] Asking for a view")
      nodes <- mem.nodes
      _     <- ZIO.foreach(nodes)(n => mem.send(CoordinatorViewRequest$, n))
      response = mem.receive
        .collect { case (_, s: CoordinatorViewResponse) => s }
        .run(ZSink.head[CoordinatorViewResponse])
        .flatMap {
          case None => UIO(false)
          case Some(s) =>
            logging.log.info(s"[CONSENSUS] Received view: ${s.view}") *> ref.set(s.view).as(true)
        }
      timeout = clock.sleep(3.seconds) *> UIO(false)
      result  <- response.race(timeout)
    } yield result

  val live: ZLayer[Membership[CoordinatorMsg] with Clock with Logging, Error, Consensus] =
    ZLayer.fromEffect {
      for {
        _           <- clock.sleep(7.seconds)
        queue       <- Queue.unbounded[Message]
        selfId      <- membership.localMember[CoordinatorMsg]
        _           <- logging.log.info(s"[CONSENSUS] Starting consensus layer for node: $selfId")
        members     <- membership.nodes[CoordinatorMsg]
        _           <- logging.log.info(s"[CONSENSUS] Present nodes: $members")
        consMembers <- Ref.make(Vector(selfId))
        _           <- ZIO.when(members.nonEmpty)(setCurrentView(consMembers).doUntilEquals(true))
        memHas      <- ZIO.environment[Membership[CoordinatorMsg]]
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
            nodes <- membership.nodes[CoordinatorMsg]
            _     <- ZIO.foreach(nodes)(n => membership.send[CoordinatorMsg](CoordinatorUserMsg(data), n))
          } yield ()).provide(memHas)
        def send(data: Chunk[Byte], to: NodeAddress): IO[Error, Unit] =
          membership.send[CoordinatorMsg](CoordinatorUserMsg(data), to).provide(memHas)
        def onLeaderChange: Stream[Error, NodeAddress] = ???
        def selfNode: UIO[NodeAddress]                 = UIO(selfId)
      }

    }

}
