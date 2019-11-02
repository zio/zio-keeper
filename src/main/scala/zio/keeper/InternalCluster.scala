package zio.keeper

import java.util.concurrent.TimeUnit

import zio.clock.Clock
import zio.console.{ Console, putStrLn }
import zio.duration._
import zio.keeper.Cluster.{ Discovery, Transport, readMessage, serializeMessage }
import zio.keeper.Error.{ ConnectionTimeout, HandshakeError, NodeUnknown, SendError }
import zio.keeper.InternalProtocol._
import zio.nio.channels.AsynchronousSocketChannel
import zio.nio.{ InetAddress, SocketAddress }
import zio.random.nextInt
import zio.stream.Stream
import zio.{ Chunk, IO, Ref, ZIO, ZSchedule }
import zio.ZManaged
import zio.UIO
import zio.Task
import zio.RIO
import zio.Queue
import zio.nio.InetSocketAddress
import java.nio.channels.Channel
import zio.stream.ZStream

final class InternalCluster(
  localMember: Member,
  nodesRef: Ref[Map[NodeId, Chunk[Byte] => UIO[Unit]]],
  gossipStateRef: Ref[GossipState],
  clusterMessageQueue: zio.Queue[Message],
  userMessageQueue: zio.Queue[Message],
  subscribeToBroadcast: UIO[Stream[Nothing, Chunk[Byte]]],
  publishToBroadCast: Chunk[Byte] => UIO[Unit],
) extends Cluster {

  private def removeMember(member: Member) =
    gossipStateRef.update(_.removeMember(member)) *> // this should be extracted to method
      nodesRef.update(_ - member.nodeId) *>
      putStrLn("remove member: " + member)

  private def startGossiping = {
    for {
      gossipState <- gossipStateRef.get
      _ <- if (gossipState.members.size > 1) {
            val gossipStateWithoutLocal = gossipState.removeMember(localMember)
            for {
              randomInt <- nextInt(gossipStateWithoutLocal.members.size)
              member    = gossipStateWithoutLocal.members.toList(randomInt)
              send      <- nodesRef.get.map(_.apply(member.nodeId))
              payload   <- ProvideClusterState(gossipState).serialize
              bytes     <- serializeMessage(localMember, payload, 1)
              _         <- send(bytes).catchAll(_ => removeMember(member))
            } yield ()
          } else {
            putStrLn("no other nodes")
          }
    } yield ()
  }.repeat(ZSchedule.fixed(5.seconds)).fork

  private def notifyJoin =
    for {
      payload <- InternalProtocol.NotifyJoin(localMember.addr).serialize
      bytes   <- serializeMessage(localMember, payload, 1)
      nodes   <- nodesRef.get

      _ <- ZIO.traversePar_(nodes) {
            case (node, send) =>
              send(bytes)
                .catchAll(ex => putStrLn("fail to send join notification: " + ex)) *>
                readMessage(channel).either
                  .flatMap {
                    case Right((1, msg)) =>
                      InternalProtocol.deserialize(msg.payload).flatMap {
                        case InternalProtocol.Ack => putStrLn("connected successfully with " + node)
                        case _ =>
                          putStrLn("unexpected response") *>
                            this.nodesRef.update(_ - node)
                      }
                    case Left(ex) =>
                      putStrLn("fail to send join notification: " + ex) *>
                        this.nodesRef.update(_ - node)
                    case Right((_, _)) =>
                      putStrLn("unexpected response") *>
                        this.nodesRef.update(_ - node)

                  }
                  .catchAll(ex => putStrLn("cannot process response for cluster join: " + ex))

          }
    } yield ()

  private def acceptConnectionRequests =
    for {
      transport <- ZManaged.environment[Transport]
      server    <- transport.bind(localMember.addr).orDie
      loop = server.accept.use { channel =>
        for {
          msg <- readMessage(channel)
          connectionRequest <- OpenConnection.deserialize(msg._2.payload)
          _ <- connect(connectionRequest.member.addr).fork
        } yield ()
      }
      _ <- loop.forever.toManaged_
    } yield ()

  private def connect(
    addr: SocketAddress
  ): ZIO[Transport with Console with Clock, Throwable, Unit] = for {
    transport <- ZIO.environment[Transport]
    _ <- transport
      .connect(addr)
      .timeout(Duration(10, TimeUnit.SECONDS))
      .use { channelOpt =>
        channelOpt.fold[ZIO[Transport with Console with Clock, Throwable, Unit]](
          putStrLn(s"Failed initiating connection with node [ ${addr} ]")
        )( channel =>
          for {
            member <- handShake(channel)
            msgBox <- listenOnChannel(channel)
            _ <- nodesRef.update(_ + (member.nodeId -> ((b: Chunk[Byte]) => msgBox.offer(b).unit.catchSomeCause { case c if (c.interrupted) => ZIO.unit })))
          } yield ()
        )
      }
    } yield ()

  private def handShake(
    channel: AsynchronousSocketChannel
  ): UIO[Member] = ???

  private def listenOnChannel(
    channel: AsynchronousSocketChannel
  ) =
    for {
      clusterMessageQueue <- Queue.bounded[Message](1000)
      userMessageQueue <- Queue.bounded[Message](1000)
      _ <- accept0(channel, clusterMessageQueue, userMessageQueue).fork
      broadcasted <- subscribeToBroadcast
      privateQueue <- Queue.bounded[Chunk[Byte]](1000)
      _ <- handleSends(broadcasted.interleave(ZStream.fromQueue(privateQueue)), channel).fork
      _ <- handleClusterMessages(clusterMessageQueue, channel).fork
    } yield privateQueue

  private def accept0(
    channel: AsynchronousSocketChannel,
    clusterMessageQueue: Queue[Message],
    userMessageQueue: Queue[Message]
  ) = {
    val loop = readMessage(channel).flatMap { case (msgType, msg) =>
      if (msgType == 1) {
        clusterMessageQueue.offer(msg).unit
      } else if (msgType == 2) {
        userMessageQueue.offer(msg).unit
      } else {
        //this should be dead letter
        putStrLn("unsupported message type")
      }
    }.catchAll { ex => putStrLn("channel close because of: " + ex.toString) }
    loop
      .orDie
      .forever
  }

  private def handleSends(messages: Stream[Nothing, Chunk[Byte]], channel: AsynchronousSocketChannel) =
    messages.tap { bytes =>
      channel
        .write(bytes)
        .catchAll(ex => putStrLn("fail to send join notification: " + ex))
    }.runDrain

  private def handleClusterMessages(queue: Queue[Message], replyTo: AsynchronousSocketChannel) = {
    val loop =
    for {
      message <- queue.take
      payload <- InternalProtocol.deserialize(message.payload)
      _ <- putStrLn(payload.toString())
      _ <- payload match {
        case InternalProtocol.NotifyJoin(inetSocketAddress) =>
          connect(inetSocketAddress) *>
            Ack.serialize.flatMap(serializeMessage(localMember, _, 1)) >>= replyTo.write
        case RequestClusterState =>
          for {
            currentClusterState <- gossipStateRef.get
            payload             <- ProvideClusterState(currentClusterState).serialize
            bytes               <- serializeMessage(localMember, payload, 1)
            _                   <- replyTo.write(bytes)
          } yield ()
        case ProvideClusterState(state) =>
          for {
            currentClusterState  <- gossipStateRef.get
            diff                  = currentClusterState.diff(state)
            _                    <- ZIO.foreach_(diff.local)(member => removeMember(member))
            _                    <- ZIO.foreach_(diff.remote)(member => connect(member.addr))
          } yield ()
        case _ => putStrLn("unknown message: " + payload)
      }
    } yield ()
    loop.forever
  }

  override def nodes =
    nodesRef.get
      .map(_.keys.toList)

  override def send(data: Chunk[Byte], receipt: NodeId): IO[Error, Unit] =
    for {
      nodes          <- nodesRef.get
      send           <- ZIO.fromEither(nodes.get(receipt).toRight(NodeUnknown(receipt)))
      payload        <- serializeMessage(localMember, data, 2)
      _              <- send(payload)
    } yield ()

  override def broadcast(data: Chunk[Byte]): IO[Error, Unit] =
    serializeMessage(localMember, data, 2).flatMap[Any, Error, Unit](publishToBroadCast).unit.orDie

  override def receive: Stream[Error, Message] =
    zio.stream.Stream.fromQueue(userMessageQueue)

}

object InternalCluster {

  private[keeper] def initCluster(port: Int) =
    for {
      localHost <- InetAddress.localHost.toManaged_.orDie
      socketAddress <- SocketAddress
                        .inetSocketAddress(localHost, port)
                        .toManaged_
                        .orDie
      localMember = Member(NodeId.generateNew, socketAddress)
      nodes       <- zio.Ref.make(Map.empty[NodeId, AsynchronousSocketChannel]).toManaged_
      seeds       <- ZManaged.environment[Discovery with Console].flatMap(d => ZManaged.fromEffect(d.discover))
      _           <- putStrLn("seeds: " + seeds).toManaged_
      newState <- ZIO.foldLeft(seeds)(GossipState.Empty) {
        case (acc, ip) =>
          connectToSeed(localMember, ip)
            .map(acc.merge)
            .catchAll(
              ex =>
                putStrLn("seed [" + ip + "] ignored because of: " + ex.getMessage)
                  .as(acc)
            )
        }.toManaged_
      _ <- ZManaged
            .foreach(newState.members) { m =>
              for {
                transport <- ZManaged.environment[Transport]
                _ <- putStrLn(s"connecting to: $m").toManaged_
                out <- transport.connect(m.addr)
                      .timeout(Duration(10, TimeUnit.SECONDS))
                      .flatMap(_.fold[ZManaged[Clock, Throwable, Unit]](ZManaged.fail(ConnectionTimeout(m.addr)))(channel => nodes.update(_ + (m.nodeId -> channel)).toManaged_.unit))
                _ <- putStrLn(s"connected to: ${m.addr}").toManaged_
              } yield out
            }
            .orDie
      clusterMessagesQueue <- zio.Queue.bounded[Message](1000).toManaged_
      userMessagesQueue    <- zio.Queue.bounded[Message](1000).toManaged_
      gossipState          <- Ref.make(GossipState(Set(localMember)).merge(newState)).toManaged_

      cluster = new InternalCluster(
        localMember,
        nodes,
        gossipState,
        clusterMessagesQueue,
        userMessagesQueue
      )
      _ <- cluster.accept
      _ <- cluster.notifyJoin.toManaged_
      _ <- cluster.listenForClusterMessages
      _ <- cluster.startGossiping.toManaged_
      _ <- putStrLn("Init cluster completed").toManaged_
    } yield cluster

  private def connectToSeed(me: Member, seed: SocketAddress) =
    ZManaged
      .environment[Transport]
      .flatMap(_.connect(seed))
      .timeout(Duration(10, TimeUnit.SECONDS))
      .flatMap(_.fold[ZManaged[Transport, Throwable, AsynchronousSocketChannel]](
        ZManaged.fail(ConnectionTimeout(seed)))(
        ZManaged.succeed
      )).use { channel =>
        for {
          //initial handshake
          _     <- putStrLn("starting handshake")
          bytes <- RequestClusterState.serialize.flatMap(serializeMessage(me, _, 1))
          _     <- channel.write(bytes)
          _     <- putStrLn("foo")
          msg   <- readMessage(me, ZManaged.succeed(channel))
          _     <- putStrLn("bar")
          m <- InternalProtocol.deserialize(msg._2.payload)
          res <- m match {
                  case InternalProtocol.ProvideClusterState(gossipState) =>
                    ZIO.succeed(gossipState) <* putStrLn("retrieved state from seed: " + seed)
                  case _ =>
                    ZIO.fail(HandshakeError("handshake error for " + seed))
                }
        } yield res
      }
}
