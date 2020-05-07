package zio.keeper.membership.hyparview.testing

import java.math.BigInteger
import java.nio.ByteBuffer

import zio._
import zio.keeper.SerializationError.{ DeserializationTypeError, SerializationTypeError }
import zio.keeper.membership.hyparview.ActiveProtocol._
import zio.keeper.membership.hyparview.PeerEvent._
import zio.keeper.membership.hyparview.{ ActiveProtocol, PeerEvent, PeerService }
import zio.keeper.transport.Transport
import zio.keeper.{ ByteCodec, Error, NodeAddress, SendError, TaggedCodec }
import zio.stm.{ STM, TQueue, TRef, ZSTM }
import zio.stream.{ Take, ZStream }

object TestPeerService {

  trait Service {
    def setPeers(peers: Set[NodeAddress]): STM[Nothing, Unit]
  }

  final case class Envelope(
    sender: NodeAddress,
    msg: ActiveProtocol.PlumTreeProtocol
  )

  object Envelope {

    implicit val codec: ByteCodec[Envelope] =
      new ByteCodec[Envelope] {

        override def fromChunk(chunk: Chunk[Byte]): IO[DeserializationTypeError, Envelope] =
          for {
            senderL <- ZIO
                        .effect(new BigInteger(chunk.take(4).toArray).intValue())
                        .mapError(_ => DeserializationTypeError("Failed reading length"))
            sender <- ByteCodec.encode[NodeAddress](chunk.drop(4).take(senderL))
            msgRaw <- TaggedCodec.read[ActiveProtocol](chunk.drop(4 + senderL))
            msg <- msgRaw match {
                    case m: ActiveProtocol.PlumTreeProtocol => ZIO.succeed(m)
                    case m                                  => ZIO.fail(DeserializationTypeError(s"Invalid message type ${m.getClass}"))
                  }
          } yield Envelope(sender, msg)

        override def toChunk(a: Envelope): IO[SerializationTypeError, Chunk[Byte]] =
          for {
            sender  <- ByteCodec.decode(a.sender)
            msg     <- TaggedCodec.write[ActiveProtocol](a.msg)
            senderL = Chunk.fromArray(ByteBuffer.allocate(4).putInt(sender.length).array())
          } yield (senderL ++ sender ++ msg)
      }
  }

  def setPeers(peers: Set[NodeAddress]): ZSTM[TestPeerService, Nothing, Unit] =
    ZSTM.accessM(_.get.setPeers(peers))

  def make(
    identifier: NodeAddress,
    eventsBuffer: Int = 128,
    messagesBuffer: Int = 128,
    concurrentConnections: Int = 16
  )(
    implicit ev: ByteCodec[Envelope]
  ): ZLayer[Transport, Nothing, TestPeerService with PeerService] = ZLayer.fromManagedMany {
    for {
      env         <- ZManaged.environment[Transport]
      ref         <- TRef.make(Set.empty[NodeAddress]).commit.toManaged_
      eventsQueue <- TQueue.bounded[PeerEvent](eventsBuffer).commit.toManaged_
      msgsQueue <- Queue
                    .bounded[Take[Error, (NodeAddress, PlumTreeProtocol)]](messagesBuffer)
                    .toManaged_
      _ <- Transport
            .bind(identifier)
            .flatMapPar[Transport, Error, (NodeAddress, PlumTreeProtocol)](concurrentConnections) { channel =>
              channel.receive
                .mapM(ByteCodec.encode[Envelope](_).map(envelope => (envelope.sender, envelope.msg)))
                .orElse(ZStream.empty)
            }
            .into(msgsQueue)
            .toManaged_
            .fork
    } yield Has.allOf[Service, PeerService.Service](
      new Service {
        override def setPeers(peers: Set[NodeAddress]): STM[Nothing, Unit] =
          for {
            oldPeers <- ref.modify((_, peers))
            _        <- eventsQueue.offerAll(oldPeers.diff(peers).map(NeighborDown.apply).toList)
            _        <- eventsQueue.offerAll(peers.diff(oldPeers).map(NeighborUp.apply).toList)
          } yield ()
      },
      new PeerService.Service {
        override val identity: ZIO[Any, Nothing, NodeAddress] = ZIO.succeed(identifier)

        override val getPeers: ZIO[Any, Nothing, Set[NodeAddress]] =
          ref.get.commit

        override def send(to: NodeAddress, message: PlumTreeProtocol): ZIO[Any, SendError, Unit] =
          ref.get.map(_.contains(to)).commit.flatMap {
            case false => ZIO.fail(SendError.NotConnected)
            case true =>
              ByteCodec
                .decode(Envelope(identifier, message))
                .mapError(SendError.SerializationFailed)
                .flatMap(Transport.send(to, _).mapError(SendError.TransportFailed))
                .provide(env)
          }

        override val receive: ZStream[Any, Error, (NodeAddress, PlumTreeProtocol)] =
          ZStream.fromQueue(msgsQueue).unTake

        override val events: ZStream[Any, Nothing, PeerEvent] =
          ZStream.fromEffect(eventsQueue.take.commit)
      }
    )
  }
}
