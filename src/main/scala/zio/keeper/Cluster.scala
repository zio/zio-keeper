package zio.keeper

import java.math.BigInteger
import java.util.UUID

import zio._
import zio.console.putStrLn
import zio.keeper.Cluster.InternalProtocol.{ ProvideIdentity, RequestIdentity }
import zio.keeper.Error.{ HandshakeError, NodeUnknown, SendError, SerializationError }
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

  sealed trait InternalProtocol {
    def serialize: IO[SerializationError, Chunk[Byte]]
  }

  object InternalProtocol {

    case object RequestIdentity extends InternalProtocol {
      override def serialize: IO[SerializationError, Chunk[Byte]] = ZIO.succeed(Chunk(1))
    }

    final case class ProvideIdentity(node: NodeId) extends InternalProtocol {
      override def serialize: IO[SerializationError, Chunk[Byte]] =
        ZIO.succeed(Chunk(2.toByte) ++ Chunk.fromArray(node.value.toString.getBytes))
    }

    final case class NotifyJoin(addr: InetAddress, port: Int) extends InternalProtocol {
      override def serialize: IO[SerializationError, Chunk[Byte]] =
        ZIO.succeed(
          Chunk(3.toByte) ++
            Chunk.fromArray(addr.address) ++
            Chunk.fromArray(BigInteger.valueOf(port.toLong).toByteArray)
        )
    }

    def deserialize(bytes: Chunk[Byte]): ZIO[Any, Throwable, InternalProtocol] =
      if (bytes.isEmpty) {
        ZIO.fail(SerializationError("Fail to deserialize message"))
      } else {
        val messageByte = bytes.drop(1)
        bytes.apply(0) match {
          case 1 =>
            ZIO.succeed(RequestIdentity)
          case 2 =>
            ZIO.effect(ProvideIdentity(NodeId(UUID.fromString(new String(messageByte.toArray)))))
          case 3 =>
            for {
              a   <- InetAddress.byAddress(messageByte.take(4).toArray)
              res <- ZIO.effect(new BigInteger(messageByte.drop(4).toArray).intValue())
            } yield NotifyJoin(a, res)
        }
      }

  }

  private def connectToSeed(publicAddr: InetAddress, port: Int, me: NodeId, seed: SocketAddress) =
    for {
      channel <- ZIO.accessM[Transport](_.connect(seed))
      //initial handshake
      bytes       <- RequestIdentity.serialize
      ss          <- serializeMessage(me, bytes, 1)
      _           <- channel.write(ss)
      headerBytes <- channel.read(24)
      byteBuffer  <- Buffer.byte(headerBytes)
      l1          <- byteBuffer.getLong
      l2          <- byteBuffer.getLong
      _           <- byteBuffer.getInt
      payloadSize <- byteBuffer.getInt
      payloadByte <- channel.read(payloadSize) // this should somehow be ignored when we cannot handle message because we are not able to handle protocol or version
      _           = NodeId(new java.util.UUID(l1, l2))
      m           <- InternalProtocol.deserialize(payloadByte)
      res <- m match {
              case InternalProtocol.ProvideIdentity(nodeId) =>
                for {
                  payload <- InternalProtocol.NotifyJoin(publicAddr, port).serialize
                  bytes   <- serializeMessage(me, payload, 1)
                  _       <- channel.write(bytes)
                } yield (nodeId, channel)
              case _ => ZIO.fail(HandshakeError("handshake error"))
            }
    } yield res

  private def listenForClusterMessages(
    currentNode: NodeId,
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
                                 "connected with node [" + message.sender.value + "] " + inetSocketAddress.hostname + ":" + port
                               )
                           }
                           .orDie
                _ <- initialNodes.update(_.updated(message.sender, client)) //TODO here we should propagate membership event
              } yield ()
            case RequestIdentity =>
              for {
                payload <- ProvideIdentity(currentNode).serialize
                bytes   <- serializeMessage(currentNode, payload, 1)
                _       <- message.replyTo.write(bytes)
              } yield ()
            case _ => ZIO.unit
          }
    } yield ()
  }.forever.fork

  private def connectToCluster(localHost: InetAddress, port: Int, me: NodeId) =
    for {
      nodes <- zio.Ref.make(Map.empty[NodeId, AsynchronousSocketChannel])
      seeds <- ZIO.accessM[Discovery](_.discover)
      _ <- ZIO.foreach(seeds) { ip =>
            connectToSeed(localHost, port, me, ip)
              .flatMap(newNode => nodes.update(_ + newNode))
              .catchAll(ex => putStrLn("seed [" + ip + "] ignored because of: " + ex.getMessage)) //we log this
          }
      internalMessagesQueue <- zio.Queue.bounded[Message](1000)
      _                     <- listenForClusterMessages(me, internalMessagesQueue, nodes)

    } yield (nodes, internalMessagesQueue)

  private val HeaderSize = 24

  private def serializeMessage(nodeId: NodeId, payload: Chunk[Byte], messageType: Int): IO[Error, Chunk[Byte]] = {
    for {
      byteBuffer <- Buffer.byte(HeaderSize + payload.length)
      _          <- byteBuffer.putLong(nodeId.value.getMostSignificantBits)
      _          <- byteBuffer.putLong(nodeId.value.getLeastSignificantBits)
      _          <- byteBuffer.putInt(messageType)
      _          <- byteBuffer.putInt(payload.length)
      _          <- byteBuffer.putChunk(payload)
      _          <- byteBuffer.flip
      bytes      <- byteBuffer.getChunk()
    } yield bytes
  }.catchAll(ex => ZIO.fail(SerializationError(ex.getMessage)))

  def join[A](
    port: Int
  ): ZIO[Credentials with Discovery with Transport with zio.console.Console, Error, Cluster] =
    for {
      localHost <- InetAddress.localHost.orDie
      socketAddress <- SocketAddress
                        .inetSocketAddress(localHost, port)
                        .orDie // this should configurable especially for docker port forwarding this might be important
      transport                     <- ZIO.environment[Transport]
      server                        <- transport.bind(socketAddress).orDie
      _                             <- putStrLn("Listening on " + localHost.hostname + ": " + port)
      currentNodeId                 = NodeId(UUID.randomUUID())
      nodesAndInternalMessagesQueue <- connectToCluster(localHost, port, currentNodeId)
      userMessagesQueue             <- zio.Queue.bounded[Message](1000)

      _ <- server.accept
            .flatMap { channel =>
              {
                for {
                  headerBytes            <- channel.read(HeaderSize)
                  byteBuffer             <- Buffer.byte(headerBytes)
                  senderMostSignificant  <- byteBuffer.getLong
                  senderLeastSignificant <- byteBuffer.getLong
                  messageType            <- byteBuffer.getInt
                  payloadSize            <- byteBuffer.getInt
                  payloadByte            <- channel.read(payloadSize)
                  sender                 = NodeId(new java.util.UUID(senderMostSignificant, senderLeastSignificant))
                  message                = Message(sender, payloadByte, channel)
                  _ <- if (messageType == 1) {
                        nodesAndInternalMessagesQueue._2.offer(message).unit
                      } else if (messageType == 2) {
                        userMessagesQueue.offer(message).unit
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
      _ <- putStrLn("Node [" + currentNodeId.value + "] started.")
    } yield make(
      currentNodeId,
      userMessagesQueue,
      nodesAndInternalMessagesQueue._1
    )

  private def make(
    me: NodeId,
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

  trait Credentials {
    // TODO: ways to obtain auth data
  }

  trait Discovery {
    def discover: IO[Error, Set[zio.nio.SocketAddress]]
  }

  trait Transport {
    def bind(publicAddress: InetSocketAddress): Task[AsynchronousServerSocketChannel]

    def connect(ip: SocketAddress): Task[AsynchronousSocketChannel]
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
