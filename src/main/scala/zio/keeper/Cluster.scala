package zio.keeper

import java.math.BigInteger
import java.util.UUID
import java.util.concurrent.TimeUnit

import zio._
import zio.clock.Clock
import zio.console.{ Console, putStrLn }
import zio.duration._
import zio.keeper.Cluster.InternalProtocol.{ Ack, ProvideClusterState, RequestClusterState }
import zio.keeper.Error._
import zio.nio._
import zio.nio.channels.{ AsynchronousServerSocketChannel, AsynchronousSocketChannel }
import zio.stream.Stream

trait Cluster {
  def nodes: UIO[List[NodeId]]

  def send(data: Chunk[Byte], receipt: NodeId): IO[Error, Unit]

  def broadcast(data: Chunk[Byte]): IO[Error, Unit]

  def receive: Stream[Error, Message]
}

object Cluster {

  private val HeaderSize = 24

  def join[A](
    port: Int
  ): ZIO[Credentials with Discovery with Transport with zio.console.Console with zio.clock.Clock, Error, Cluster] =
    for {
      localHost <- InetAddress.localHost.orDie
      socketAddress <- SocketAddress
                        .inetSocketAddress(localHost, port)
                        .orDie // this should configurable especially for docker port forwarding this might be important
      transport                     <- ZIO.environment[Transport]
      localMember                 = Member(NodeId(UUID.randomUUID()), socketAddress)
      membersAndInternalMessagesQueue <- connectToCluster(localMember)
      userMessagesQueue             <- zio.Queue.bounded[Message](1000)
      gossipStateRef <- Ref.make(GossipState.Empty)

      server <- transport.bind(socketAddress).orDie
      _      <- putStrLn("Listening on " + localHost.hostname + ": " + port)
      _ <- server.accept
            .flatMap { channel =>
              {
                for {
                  typeAndMessage <- readMessage(channel)
                  _ <- if (typeAndMessage._1 == 1) {
                        membersAndInternalMessagesQueue._2.offer(typeAndMessage._2).unit
                      } else if (typeAndMessage._1 == 2) {
                        userMessagesQueue.offer(typeAndMessage._2).unit
                      } else {
                        //this should be dead letter
                        putStrLn("unsupported message type")
                      }
                } yield ()
              }.whenM(channel.isOpen)
                .forever
                .ensuring(channel.close.ignore *> putStrLn("channel closed"))
                .catchAll(ex => putStrLn("channel close because of: " + ex.getMessage)) //we log this
                .fork                                                                   //this is individual channel for established connection
            }
            .orDie
            .forever
            .fork //this is waiting for new connections

      payload <- InternalProtocol.NotifyJoin(localHost, port).serialize
      bytes   <- serializeMessage(localMember, payload, 1)
      nodes   <- membersAndInternalMessagesQueue._1.get

      _ <- ZIO.traversePar_(nodes) {
            case (node, channel) =>
              channel
                .write(bytes)
                .catchAll(ex => putStrLn("fail to send join notification: " + ex)) *>
                readMessage(channel).either
                  .flatMap {
                    case Right((1, msg)) =>
                      InternalProtocol.deserialize(msg.payload).flatMap {
                        case InternalProtocol.Ack => putStrLn("connected successfully with " + node)
                        case _ =>
                          putStrLn("unexpected response") *>
                            membersAndInternalMessagesQueue._1.update(_ - node)
                      }
                    case Left(ex) =>
                      putStrLn("fail to send join notification: " + ex) *>
                        membersAndInternalMessagesQueue._1.update(_ - node)
                    case Right((_, _)) =>
                      putStrLn("unexpected response") *>
                        membersAndInternalMessagesQueue._1.update(_ - node)

                  }
                  .catchAll(ex => putStrLn("cannot process response for cluster join: " + ex))

          }
      _ <- listenForClusterMessages(
            localMember,
            gossipStateRef,
            membersAndInternalMessagesQueue._2,
            membersAndInternalMessagesQueue._1
          )

      _ <- putStrLn("Node [" + localMember.nodeId + "] started.")
    } yield make(
      localMember,
      userMessagesQueue,
      membersAndInternalMessagesQueue._1
    )

  private def listenForClusterMessages(
                                        currentNode: Member,
                                        gossipState: Ref[GossipState],
                                        messageQueue: zio.Queue[Message],
                                        initialNodes: Ref[Map[NodeId, AsynchronousSocketChannel]]
  ) = {
    for {
      message <- messageQueue.take
      payload <- InternalProtocol.deserialize(message.payload)
      _ <- payload match {
            case InternalProtocol.NotifyJoin(inetSocketAddress, port) =>
              for {
                client <- ZIO
                           .accessM[Transport with zio.console.Console] { transport =>
                             (SocketAddress.inetSocketAddress(inetSocketAddress, port) >>=
                               transport.connect) <*
                               putStrLn(
                                 "connected with node [" + message.sender + "] " + inetSocketAddress.hostname + ":" + port
                               )
                           }
                           .orDie
                _ <- initialNodes.update(_.updated(message.sender, client)) //TODO here we should propagate membership event
                _ <- Ack.serialize >>=
                      (serializeMessage(currentNode, _: Chunk[Byte], 1)) >>=
                      message.replyTo.write
              } yield ()
            case RequestClusterState =>
              for {
                currentClusterState <- gossipState.get
                payload <- ProvideClusterState(currentClusterState).serialize
                bytes   <- serializeMessage(currentNode, payload, 1)
                _       <- message.replyTo.write(bytes)
              } yield ()
            case _ => ZIO.unit
          }
    } yield ()
  }.forever.fork

  private def connectToCluster(me: Member) =
    for {
      nodes <- zio.Ref.make(Map.empty[NodeId, AsynchronousSocketChannel])
      seeds <- ZIO.accessM[Discovery with Console](_.discover)
      _     <- putStrLn("seeds: " + seeds)
      newState <- ZIO.foldLeft(seeds)(GossipState.Empty) { case (acc, ip) =>
            connectToSeed(me, ip)
              .map(acc.merge)
              .catchAll(ex =>
                putStrLn("seed [" + ip + "] ignored because of: " + ex.getMessage)
                  .as(acc)) //we log this
          }
      _ <- ZIO.foreach(newState.members){m =>
        ZIO.accessM[Transport with Clock with zio.console.Console](
          putStrLn("connecting to: " + m) *>
          _.connect(m.addr).timeoutFail(ConnectionTimeout(m.addr))(Duration(10, TimeUnit.SECONDS))
          <* putStrLn("connected to: " + m.addr)
        ).flatMap(channel => nodes.update(_ + (m.nodeId -> channel)))
      }.orDie
      internalMessagesQueue <- zio.Queue.bounded[Message](1000)

    } yield (nodes, internalMessagesQueue)

  private def connectToSeed(me: Member, seed: SocketAddress) =
    for {
      channel <- ZIO.accessM[Transport with Clock with zio.console.Console](
                  putStrLn("connecting to: " + seed) *>
                    _.connect(seed).timeoutFail(ConnectionTimeout(seed))(Duration(10, TimeUnit.SECONDS))
                    <* putStrLn("connected to: " + seed)
                )
      //initial handshake
      _     <- putStrLn("starting handshake")
      bytes <- RequestClusterState.serialize.flatMap(serializeMessage(me, _, 1))
      _     <- channel.write(bytes)
      msg   <- readMessage(channel)

      m <- InternalProtocol.deserialize(msg._2.payload)
      res <- m match {
              case InternalProtocol.ProvideClusterState(gossipState) =>
                ZIO.succeed(gossipState) <* putStrLn("retrieved state from seed: " + seed)
              case _ =>
                ZIO.fail(HandshakeError("handshake error for " + seed))
            }
    } yield res

  private def readMessage(channel: AsynchronousSocketChannel) =
    for {
      headerBytes <- channel
                      .read(HeaderSize, Duration(10, TimeUnit.SECONDS))
      byteBuffer             <- Buffer.byte(headerBytes)
      senderMostSignificant  <- byteBuffer.getLong
      senderLeastSignificant <- byteBuffer.getLong
      messageType            <- byteBuffer.getInt
      payloadSize            <- byteBuffer.getInt
      payloadByte <- channel
                      .read(payloadSize, Duration(10, TimeUnit.SECONDS))
      sender = NodeId(new java.util.UUID(senderMostSignificant, senderLeastSignificant))
    } yield (messageType, Message(sender, payloadByte, channel))

  private def make(
                    me: Member,
                    q: zio.Queue[Message],
                    initialNodes: Ref[Map[NodeId, AsynchronousSocketChannel]]
  ) = new Cluster {
    override def nodes =
      initialNodes.get
        .map(_.keys.toList) // should get from initial handshake( going through discovery list and try to connect) then gossip maintain this list

    override def send(data: Chunk[Byte], receipt: NodeId): IO[Error, Unit] =
      for {
        nodes          <- initialNodes.get
        receiptChannel <- ZIO.fromEither(nodes.get(receipt).toRight(NodeUnknown(receipt)))
        payload        <- serializeMessage(me, data, 2)
        _ <- receiptChannel
              .write(payload)
              .catchAll(ex => ZIO.fail(SendError(receipt, data, ex.getMessage)))
      } yield ()

    override def broadcast(data: Chunk[Byte]): IO[Error, Unit] =
      for {
        nodes   <- initialNodes.get
        payload <- serializeMessage(me, data, 2).orDie
        _ <- ZIO.traversePar_(nodes) {
              case (receipt, channel) =>
                channel
                  .write(payload)
                  .catchAll(ex => ZIO.fail(SendError(receipt, data, ex.getMessage)))
            }
      } yield ()

    override def receive: Stream[Error, Message] =
      zio.stream.Stream.fromQueue(q)
  }

  private def serializeMessage(member: Member, payload: Chunk[Byte], messageType: Int): IO[Error, Chunk[Byte]] = {
    for {
      byteBuffer <- Buffer.byte(HeaderSize + payload.length)
      _          <- byteBuffer.putLong(member.nodeId.value.getMostSignificantBits)
      _          <- byteBuffer.putLong(member.nodeId.value.getLeastSignificantBits)
      _          <- byteBuffer.putInt(messageType)
      _          <- byteBuffer.putInt(payload.length)
      _          <- byteBuffer.putChunk(payload)
      _          <- byteBuffer.flip
      bytes      <- byteBuffer.getChunk()
    } yield bytes
  }.catchAll(ex => ZIO.fail(SerializationError(ex.getMessage)))

  sealed trait InternalProtocol {
    def serialize: IO[SerializationError, Chunk[Byte]]
  }

  trait Credentials {
    // TODO: ways to obtain auth data
  }

  trait Discovery {
    def discover: ZIO[Console, Error, Set[zio.nio.SocketAddress]]
  }

  trait Transport {
    def bind(publicAddress: InetSocketAddress): Task[AsynchronousServerSocketChannel]

    def connect(ip: SocketAddress): Task[AsynchronousSocketChannel]
  }

  object InternalProtocol {

    private val RequestIdentityMsgId: Byte = 1
    private val ProvideIdentityMsgId: Byte = 2
    private val NotifyJoinMsgId: Byte      = 3
    private val AckMsgId: Byte             = 4

    def deserialize(bytes: Chunk[Byte]): ZIO[Any, Throwable, InternalProtocol] =
      if (bytes.isEmpty) {
        ZIO.fail(SerializationError("Fail to deserialize message"))
      } else {
        val messageByte = bytes.drop(1)
        bytes.apply(0) match {
          case RequestIdentityMsgId =>
            ZIO.succeed(RequestClusterState)
          case ProvideIdentityMsgId =>
            ???
            //ZIO.effect(ProvideClusterState(Member(UUID.fromString(new String(messageByte.toArray)))))//this should using UUID longs
          case NotifyJoinMsgId =>
            for {
              a   <- InetAddress.byAddress(messageByte.take(4).toArray)
              res <- ZIO.effect(new BigInteger(messageByte.drop(4).toArray).intValue())
            } yield NotifyJoin(a, res)
          case AckMsgId =>
            ZIO.succeed(Ack)
        }
      }

    final case class ProvideClusterState(node: GossipState) extends InternalProtocol {
      override def serialize: IO[SerializationError, Chunk[Byte]] =
//        for {
//          byteBuffer <- Buffer.byte()
//
//        } yield
//    }
//        ZIO.succeed(Chunk(ProvideIdentityMsgId.toByte) ++ Chunk.succeed(node.members.size) ++ node.members.foldLeft(Chunk.empty){case (acc, node) => Chunk.fromArray(Array())

    ???
    }

    final case class NotifyJoin(addr: InetAddress, port: Int) extends InternalProtocol {
      override def serialize: IO[SerializationError, Chunk[Byte]] =
        ZIO.succeed(
          Chunk(NotifyJoinMsgId.toByte) ++
            Chunk.fromArray(addr.address) ++
            Chunk.fromArray(BigInteger.valueOf(port.toLong).toByteArray)
        )
    }

    case object RequestClusterState extends InternalProtocol {
      override val serialize: IO[SerializationError, Chunk[Byte]] =
        ZIO.succeed(Chunk(RequestIdentityMsgId))
    }

    case object Ack extends InternalProtocol {
      override val serialize: IO[SerializationError, Chunk[Byte]] =
        ZIO.succeed(Chunk(AckMsgId))
    }

  }

  object Transport {

    trait TCPTransport extends Transport {
      override def bind(publicAddress: InetSocketAddress): Task[AsynchronousServerSocketChannel] =
        for {
          socket <- AsynchronousServerSocketChannel().orDie
          _      <- socket.bind(publicAddress).orDie
        } yield socket

      override def connect(ip: SocketAddress): Task[AsynchronousSocketChannel] =
        for {
          client <- AsynchronousSocketChannel()
          _      <- client.connect(ip)
        } yield client
    }

  }

}
