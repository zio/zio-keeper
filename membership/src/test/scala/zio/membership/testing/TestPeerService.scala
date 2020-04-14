package zio.membership.testing

import java.math.BigInteger
import java.nio.ByteBuffer

import zio._
import zio.keeper.SerializationError.{ DeserializationTypeError, SerializationTypeError }
import zio.keeper.membership.{ ByteCodec, TaggedCodec }
import zio.membership.PeerEvent.{ NeighborDown, NeighborUp }
import zio.membership.hyparview.ActiveProtocol
import zio.membership.hyparview.ActiveProtocol.PlumTreeProtocol
import zio.membership.transport.Transport
import zio.membership.{ PeerEvent, PeerService, SendError }
import zio.stm._
import zio.stream.{ Take, ZStream }

object TestPeerService {

  trait Service[T] {
    def setPeers(peers: Set[T]): STM[Nothing, Unit]
  }

  final case class Envelope[T](
    sender: T,
    msg: ActiveProtocol.PlumTreeProtocol
  )

  object Envelope {

    implicit def codec[T: ByteCodec](implicit ev: TaggedCodec[ActiveProtocol[T]]): ByteCodec[Envelope[T]] =
      new ByteCodec[Envelope[T]] {

        override def fromChunk(chunk: Chunk[Byte]): IO[DeserializationTypeError, Envelope[T]] =
          for {
            senderL <- ZIO
                        .effect(new BigInteger(chunk.take(4).toArray).intValue())
                        .mapError(_ => DeserializationTypeError("Failed reading length"))
            sender <- ByteCodec.encode[T](chunk.drop(4).take(senderL))
            msgRaw <- TaggedCodec.read[ActiveProtocol[T]](chunk.drop(4 + senderL))
            msg <- msgRaw match {
                    case m: ActiveProtocol.PlumTreeProtocol => ZIO.succeed(m)
                    case m                                  => ZIO.fail(DeserializationTypeError(s"Invalid message type ${m.getClass()}"))
                  }
          } yield Envelope(sender, msg)

        override def toChunk(a: Envelope[T]): IO[SerializationTypeError, Chunk[Byte]] =
          for {
            sender  <- ByteCodec.decode(a.sender)
            msg     <- TaggedCodec.write[ActiveProtocol[T]](a.msg)
            senderL = Chunk.fromArray(ByteBuffer.allocate(4).putInt(sender.length).array())
          } yield (senderL ++ sender ++ msg)
      }
  }

  def setPeers[T: Tagged](peers: Set[T]): ZSTM[TestPeerService[T], Nothing, Unit] =
    ZSTM.accessM(_.get.setPeers(peers))

  def make[T: Tagged](
    identifier: T,
    eventsBuffer: Int = 128,
    messagesBuffer: Int = 128,
    concurrentConnections: Int = 16
  )(
    implicit ev: ByteCodec[Envelope[T]]
  ): ZLayer[Transport[T], Nothing, TestPeerService[T] with PeerService[T]] = ZLayer.fromManagedMany {
    for {
      env         <- ZManaged.environment[Transport[T]]
      ref         <- TRef.make(Set.empty[T]).commit.toManaged_
      eventsQueue <- TQueue.bounded[PeerEvent[T]](eventsBuffer).commit.toManaged_
      msgsQueue <- Queue
                    .bounded[Take[membership.Error, (T, PlumTreeProtocol)]](messagesBuffer)
                    .toManaged_
      _ <- Transport
            .bind(identifier)
            .flatMapPar[Transport[T], membership.Error, (T, PlumTreeProtocol)](concurrentConnections) { channel =>
              channel.receive
                .mapM(ByteCodec.encode[Envelope[T]](_).map(envelope => (envelope.sender, envelope.msg)))
                .orElse(ZStream.empty)
            }
            .into(msgsQueue)
            .toManaged_
            .fork
    } yield Has.allOf[Service[T], PeerService.Service[T]](
      new Service[T] {
        override def setPeers(peers: Set[T]): STM[Nothing, Unit] =
          for {
            oldPeers <- ref.modify((_, peers))
            _        <- eventsQueue.offerAll(oldPeers.diff(peers).map(NeighborDown.apply).toList)
            _        <- eventsQueue.offerAll(peers.diff(oldPeers).map(NeighborUp.apply).toList)
          } yield ()
      },
      new PeerService.Service[T] {
        override val identity: ZIO[Any, Nothing, T] = ZIO.succeed(identifier)

        override val getPeers: ZIO[Any, Nothing, Set[T]] =
          ref.get.commit

        override def send(to: T, message: PlumTreeProtocol): ZIO[Any, SendError, Unit] =
          ref.get.map(_.contains(to)).commit.flatMap {
            case false => ZIO.fail(SendError.NotConnected)
            case true =>
              ByteCodec
                .decode(Envelope(identifier, message))
                .mapError(SendError.SerializationFailed)
                .flatMap(Transport.send(to, _).mapError(SendError.TransportFailed))
                .provide(env)
          }

        override val receive: ZStream[Any, membership.Error, (T, PlumTreeProtocol)] =
          ZStream.fromQueue(msgsQueue).unTake

        override val events: ZStream[Any, Nothing, PeerEvent[T]] =
          ZStream.fromEffect(eventsQueue.take.commit)
      }
    )
  }
}
