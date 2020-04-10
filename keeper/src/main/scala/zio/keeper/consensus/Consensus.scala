package zio.keeper.consensus

import java.util.UUID

import zio._
import zio.clock.Clock
import zio.keeper.membership.{ByteCodec, Membership, MembershipEvent, NodeId, TaggedCodec}
import zio.keeper.{Error, Message, membership}
import zio.stream.{Stream, ZSink}
import zio.keeper.protocol.InternalProtocol._
import upickle.default._
import zio.duration.Duration

//Implemented by user
object ReceiverAdapter {

  trait Service {
    def receive(data: Chunk[Byte]): IO[Error, Unit]

    def setState(data: Chunk[Byte]): IO[Error, Unit]

    def getState: IO[Error, Chunk[Byte]]
  }

}

sealed trait ConsensusMsg
final case object ConsensusViewRequest extends ConsensusMsg
final case class ConsensusViewResponse(view: List[NodeId]) extends ConsensusMsg
final case class ConsensusUserMsg(payload: Chunk[Byte]) extends ConsensusMsg

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

//consensus live
object Coordinator {

  val q120 = ByteCodec.fromReadWriter(macroRW[ConsensusViewRequest.type])
  val q122 = ByteCodec.fromReadWriter(macroRW[ConsensusViewResponse])
  val q123 = ByteCodec.fromReadWriter(
    implicitly[ReadWriter[Array[Byte]]].bimap[ConsensusUserMsg](_.payload.toArray, bA => ConsensusUserMsg(Chunk.fromArray(bA)))
  )
  implicit val xdd: TaggedCodec[ConsensusMsg] =
    TaggedCodec.instance[ConsensusMsg](
      {
        case _: ConsensusViewRequest.type => 50
        case _: ConsensusViewResponse => 51
        case _: ConsensusUserMsg => 52
      }, {
        case 50 => q120.asInstanceOf[ByteCodec[ConsensusMsg]]
        case 51 => q122.asInstanceOf[ByteCodec[ConsensusMsg]]
        case 52 => q123.asInstanceOf[ByteCodec[ConsensusMsg]]
      }
    )

  private def runEventLoop(ref: Ref[List[NodeId]], self: NodeId): ZIO[Membership, Error, Unit] =
    membership.events.foreach {
        case MembershipEvent.Join(member) =>
          ref.update(view => view :+ member.nodeId)

        case MembershipEvent.Leave(member) =>
          for {
            successor <- ref.modify { view =>
              val idx = view.indexOf(member.nodeId)
              val successor = if (idx == 0) view.tail.headOption else None
              (successor, view.filterNot(_ == member.nodeId))
            }
            _ <- successor match {
              case Some(id) if id == self =>
                for {
                  view <- ref.get
                  payload <- TaggedCodec.write[ConsensusMsg](ConsensusViewResponse(view))
                  _ <- membership.broadcast(payload)
                } yield ()
              case None =>
                UIO.unit
            }
          } yield ()

        case MembershipEvent.Unreachable(_) =>
          UIO.unit
    }

  def runMsgLoop(self: NodeId, ref: Ref[List[NodeId]], queue: Queue[Message]): ZIO[Membership, Error, Unit] =
    membership.receive.tap { msg =>
      for {
        read <- TaggedCodec.read[ConsensusMsg](msg.payload)
        _ <- read match {
        case ConsensusViewRequest =>
          for {
           _ <- console.putStrLn("Sb asked for a view").provideLayer(console.Console.live)
            view <- ref.get
            leader = view.headOption
            _ <- leader match {
              case Some(value) if value == self =>
                for {
                  payload <- TaggedCodec.write[ConsensusMsg](ConsensusViewResponse(view))
                  _ <- membership.send(payload, msg.sender)
                  _ <- console.putStrLn("Answering").provideLayer(console.Console.live)
                } yield ()
              case _ => UIO.unit
            }
          } yield ()
        case ConsensusViewResponse(view) => ref.set(view)
        case ConsensusUserMsg(payload) =>
          console.putStrLn("custom message rec").provideLayer(console.Console.live) *> queue.offer(Message(UUID.randomUUID(), msg.sender, payload))
      }
      } yield ()
    }.runDrain

  def getCurrentView(ref: Ref[List[NodeId]]) =
    for {
      mem <- ZIO.access[Membership](_.get[Membership.Service])
      clock <- ZIO.access[Clock](_.get[Clock.Service])
      payload <- TaggedCodec.write[ConsensusMsg](ConsensusViewRequest)
      _ <- console.putStrLn("Asking for view").provideLayer(console.Console.live)
      _ <- mem.broadcast(payload)
      eee = mem.receive
        .mapM(mm => console.putStrLn("Smth received").provideLayer(console.Console.live) *> TaggedCodec.read[ConsensusMsg](mm.payload))
        .collect{case s: ConsensusViewResponse => s}
        .run(ZSink.head[ConsensusViewResponse])
        .flatMap {
          case None    => UIO(false)
          case Some(s) => console.putStrLn("received view").provideLayer(console.Console.live) *>
            ref.set(s.view).map(_ => true)
        }
      www = clock.sleep(Duration.fromNanos(5000000000L)) *> UIO(false)
      result <- eee.race(www)
    } yield result

  def create(): ZLayer[Membership with Clock, Error, Consensus] =
    ZLayer.fromEffect {
      for {
        queue <- Queue.unbounded[Message]
        selfId <- membership.localMember
        members <- membership.nodes
        _ <- console.putStrLn(members.mkString(" ")).provideLayer(console.Console.live)
        consensusMembers <- Ref.make(List(selfId.nodeId))
        _ <- if (members.nonEmpty) getCurrentView(consensusMembers) else UIO.unit
        memHas <- ZIO.environment[Membership]
        _ <- runEventLoop(consensusMembers, selfId.nodeId).fork
        _ <- runMsgLoop(selfId.nodeId, consensusMembers, queue).fork
        _ <- console.putStrLn("Consensus is running").provideLayer(console.Console.live)
      } yield new Consensus.Service {
        def getLeader: UIO[NodeId] =
          consensusMembers.get.map(_.headOption.getOrElse(selfId.nodeId))
        def getView: UIO[List[NodeId]] = consensusMembers.get
        def receive: Stream[Error, Message] = Stream.fromQueue(queue)
        def broadcast(data: Chunk[Byte]): IO[Error, Unit] =
          for {
            payload <- TaggedCodec.write[ConsensusMsg](ConsensusUserMsg(data))
            _ <- membership.broadcast(payload).provide(memHas)
          } yield ()
        def send(data: Chunk[Byte], to: NodeId): IO[Error, Unit] =
          for {
            payload <- TaggedCodec.write[ConsensusMsg](ConsensusUserMsg(data))
            _ <- membership.send(payload, to).provide(memHas)
          } yield ()
        def onLeaderChange: Stream[Error, NodeId] = ???
        def selfNode: UIO[NodeId] = UIO(selfId.nodeId)
      }

    }

}
