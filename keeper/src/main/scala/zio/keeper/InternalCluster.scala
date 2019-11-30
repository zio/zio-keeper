package zio.keeper

import java.util.concurrent.TimeUnit

import zio._
import zio.clock.Clock
import zio.console.{ Console, putStrLn }
import zio.duration._
import zio.keeper.Cluster.{ readMessage, serializeMessage }
import zio.keeper.Error.{ SendError, UnexpectedMessage }
import zio.keeper.discovery.Discovery
import zio.keeper.protocol.InternalProtocol
import zio.keeper.protocol.InternalProtocol._
import zio.keeper.transport.{ ChannelOut, Transport }
import zio.nio.{ InetAddress, SocketAddress }
import zio.stm.{ STM, TMap }
import zio.stream.{ Stream, ZStream }

import scala.collection.SortedSet

final class InternalCluster(
  localMember: Member,
  nodesRef: Ref[Map[NodeId, Chunk[Byte] => UIO[Unit]]],
  gossipStateRef: Ref[GossipState],
  userMessageQueue: zio.Queue[Message],
  subscribeToBroadcast: UIO[Stream[Nothing, Chunk[Byte]]],
  publishToBroadCast: Chunk[Byte] => UIO[Unit],
  msgOffset: Ref[Long],
  acks: TMap[Long, Promise[Nothing, Unit]]
) extends Cluster {

  private def removeMember(member: Member) =
    gossipStateRef.update(_.removeMember(member)) *>
      nodesRef.update(_ - member.nodeId) *>
      putStrLn("remove member: " + member)

  private def addMember(member: Member, send: Chunk[Byte] => UIO[Unit]) =
    gossipStateRef.update(_.addMember(member)) *>
      nodesRef.update(_ + (member.nodeId -> send)) *>
      putStrLn("add member: " + member)

  private def updateState(newState: GossipState) =
    for {
      current <- gossipStateRef.get
      diff    = newState.diff(current)
      _       <- ZIO.foreach(diff.local)(n => n.addr.socketAddress >>= connect)
    } yield ()

  private def sendMessage(to: NodeId, msgType: Int, payload: Chunk[Byte]) =
    for {
      node <- nodesRef.get.map(_.get(to))
      _ <- node match {
            case Some(send) =>
              serializeMessage(localMember, payload, msgType) >>= send
            case None => ZIO.unit
          }
    } yield ()

  private def sendInternalMessage(to: ChannelOut, msg: InternalProtocol) =
    for {
      payload <- msg.serialize
      msg     <- serializeMessage(localMember, payload, 1)
      _       <- to.send(msg)
    } yield ()

  private def expects[R, A](
    channel: ChannelOut
  )(pf: PartialFunction[InternalProtocol, ZIO[R, Error, A]]): ZIO[R, Error, A] =
    for {
      bytes <- readMessage(channel)
      msg   <- InternalProtocol.deserialize(bytes._2.payload)
      result <- pf.lift(msg).getOrElse(ZIO.fail(UnexpectedMessage(bytes._2)))
    } yield result

  private def ackMessage(timeout: Duration) =
    for {
      offset <- msgOffset.update(_ + 1)
      prom   <- Promise.make[Nothing, Unit]
      _      <- acks.put(offset, prom).commit
    } yield (
      offset,
      prom.await
        .timeoutFail(())(timeout)
        .foldM(
          _ => acks.delete(offset).commit *> ZIO.fail(()),
          _ => acks.delete(offset).commit
        )
    )

  private def ack(id: Long) =
    for {
      promOpt <- acks
                  .get(id)
                  .flatMap(
                    _.fold(STM.succeed[Option[Promise[Nothing, Unit]]](None))(prom => acks.delete(id).as(Some(prom)))
                  )
                  .commit
      _ <- promOpt.fold(ZIO.unit)(_.succeed(()).unit)
    } yield ()

  private def runSwim =
    Ref.make(0).flatMap { roundRobinOffset =>
      val loop = gossipStateRef.get.map(_.members.filterNot(_ == localMember).toIndexedSeq).flatMap { nodes =>
        if (nodes.nonEmpty) {
          for {
            next           <- roundRobinOffset.update(old => if (old < nodes.size - 1) old + 1 else 0)
            state          <- gossipStateRef.get
            target         = nodes(next) // todo: insert in random position and keep going in round robin version
            ack            <- ackMessage(10.seconds)
            (ackId, await) = ack
            _              <- Ping(ackId, state).serialize.flatMap(sendMessage(target.nodeId, 1, _))
            _ <- await
                  .timeoutFail(())(10.seconds)
                  .foldM(
                    _ => // attempt req messages
                      for {
                        jumps <- ZIO.collectAll(List.fill(3)(zio.random.nextInt(nodes.size).map(nodes(_))))
                        pingReqs = jumps.map { jump =>
                          for {
                            ack            <- ackMessage(10.seconds)
                            (ackId, await) = ack
                            _              <- PingReq(target, ackId, state).serialize.flatMap(sendMessage(jump.nodeId, 1, _))
                            _              <- await
                          } yield ()
                        }
                        _ <- ZIO
                              .raceAll(ZIO.never, pingReqs)
                              .foldM(
                                _ => removeMember(target),
                                _ =>
                                  putStrLn(
                                    s"SWIM: Successful ping req to [ ${target.nodeId} ] through [ ${jumps.map(_.nodeId).mkString(", ")} ]"
                                  )
                              )
                      } yield (),
                    _ => putStrLn(s"SWIM: Successful ping to [ ${target.nodeId} ]")
                  )
          } yield ()
        } else {
          putStrLn("SWIM: No nodes to spread gossip to")
        }
      }
      loop.repeat(Schedule.spaced(30.seconds))
    }

  private def acceptConnectionRequests =
    ZManaged
      .environment[Console with Transport with Clock]
      .flatMap(
        env =>
          localMember.addr.socketAddress.toManaged_.flatMap(
            localAddress =>
              transport.bind(localAddress) { channelOut =>
                {
                  (for {
                    state <- gossipStateRef.get
                    _     <- sendInternalMessage(channelOut, OpenConnection(state, localMember))
                    _ <- expects(channelOut) {
                          case OpenConnection(_, address) => listenOnChannel(channelOut, address)
                        }
                  } yield ()).catchAll(ex => putStrLn(s"Connection failed: $ex"))
                }.provide(env)
              }
          )
      )

  private def connect(
    addr: SocketAddress
  ): ZIO[Transport with Console with Clock, Error, Unit] =
    for {
      _ <- transport
            .connect(addr)
            .timeout(Duration(10, TimeUnit.SECONDS))
            .use { channelOpt =>
              channelOpt.fold[ZIO[Transport with Console with Clock, Throwable, Unit]](
                putStrLn(s"Timed out initiating connection with node [ ${addr} ]")
              )(
                channel =>
                  putStrLn(s"Initiating handshake with node at ${addr}") *>
                    handshake(channel)
              )
            }
            .catchAll(ex => putStrLn(s"Failed initiating connection with node [ ${addr} ]: $ex"))
            .fork
    } yield ()

  private def handshake(
    channel: ChannelOut
  ): ZIO[Transport with Console with Clock, Error, Unit] =
    expects(channel) {
      case OpenConnection(_, address) =>
        (for {
          state <- gossipStateRef.get
          _     <- sendInternalMessage(channel, OpenConnection(state, localMember))
        } yield ()) *> listenOnChannel(channel, address)
    }

  private def listenOnChannel(
    channel: ChannelOut,
    partner: Member
  ) = {

    def handleSends(messages: Stream[Nothing, Chunk[Byte]]) =
      messages.tap { bytes =>
        channel
          .send(bytes)
          .catchAll(ex => ZIO.fail(SendError(partner.nodeId, bytes, ex.getMessage)))
      }.runDrain

    (for {
      _                   <- putStrLn(s"Setting up connection with [ ${partner.nodeId} ]")
      clusterMessageQueue <- Queue.bounded[Message](1000)
      _                   <- routeMessages(channel, clusterMessageQueue, userMessageQueue).fork
      broadcasted         <- subscribeToBroadcast
      privateQueue        <- Queue.bounded[Chunk[Byte]](1000)
      _ <- addMember(
            partner,
            (b: Chunk[Byte]) => privateQueue.offer(b).unit.catchSomeCause { case c if (c.interrupted) => ZIO.unit }
          )
      _ <- handleSends(broadcasted.merge(ZStream.fromQueue(privateQueue))).fork
    } yield ZStream.fromQueue(clusterMessageQueue)) >>= handleClusterMessages
  }

  private def routeMessages(
    channel: ChannelOut,
    clusterMessageQueue: Queue[Message],
    userMessageQueue: Queue[Message]
  ) = {
    val loop = readMessage(channel)
      .flatMap {
        case (msgType, msg) =>
          if (msgType == 1) {
            clusterMessageQueue.offer(msg).unit
          } else if (msgType == 2) {
            userMessageQueue.offer(msg).unit
          } else {
            //this should be dead letter
            putStrLn("unsupported message type")
          }
      }
      .catchAll { ex =>
        putStrLn(s"channel close because of: $ex")
      }
    loop.whenM(channel.isOpen.catchAll[Any, Nothing, Boolean](_ => ZIO.succeed(false))).forever
  }

  private def handleClusterMessages(stream: Stream[Nothing, Message]) =
    stream.tap { message =>
      for {
        payload <- InternalProtocol.deserialize(message.payload)
        _ <- payload match {
              case Ack(ackId, state) =>
                updateState(state) *>
                  ack(ackId)
              case Ping(ackId, state) =>
                for {
                  _     <- updateState(state)
                  state <- gossipStateRef.get
                  _     <- Ack(ackId, state).serialize.flatMap(sendMessage(message.sender, 1, _))
                } yield ()
              case PingReq(target, originalAckId, state) =>
                for {
                  _              <- updateState(state)
                  state          <- gossipStateRef.get
                  ack            <- ackMessage(10.seconds)
                  (ackId, await) = ack
                  _              <- Ping(ackId, state).serialize.flatMap(sendMessage(target.nodeId, 1, _))
                  _ <- await
                        .foldM(
                          _ => ZIO.unit,
                          _ =>
                            gossipStateRef.get
                              .flatMap(Ack(originalAckId, _).serialize.flatMap(sendMessage(message.sender, 1, _)))
                        )
                        .fork
                } yield ()
              case _ => putStrLn("unknown message: " + payload)
            }
      } yield ()
    }.runDrain

  override def nodes =
    nodesRef.get
      .map(_.keys.toList)

  override def send(data: Chunk[Byte], receipt: NodeId): IO[Error, Unit] =
    sendMessage(receipt, 2, data)

  override def broadcast(data: Chunk[Byte]): IO[Error, Unit] =
    serializeMessage(localMember, data, 2).flatMap[Any, Error, Unit](publishToBroadCast).unit

  override def receive: Stream[Error, Message] =
    zio.stream.Stream.fromQueue(userMessageQueue)

}

object InternalCluster {

  private[keeper] def initCluster(port: Int) =
    for {
      localHost         <- InetAddress.localHost.toManaged_.orDie
      localMember       = Member(NodeId.generateNew, NodeAddress(localHost.address, port))
      _                 <- putStrLn(s"Starting node [ ${localMember.nodeId} ]").toManaged_
      nodes             <- zio.Ref.make(Map.empty[NodeId, Chunk[Byte] => UIO[Unit]]).toManaged_
      seeds             <- ZManaged.environment[Discovery with Console].flatMap(d => ZManaged.fromEffect(d.discover))
      _                 <- putStrLn("seeds: " + seeds).toManaged_
      userMessagesQueue <- zio.Queue.bounded[Message](1000).toManaged_
      gossipState       <- Ref.make(GossipState(SortedSet(localMember))).toManaged_
      broadcastQueue    <- zio.Queue.bounded[Chunk[Byte]](1000).toManaged_
      subscriberBroadcast <- ZStream
                              .fromQueue(broadcastQueue)
                              .distributedWithDynamic[Nothing, Chunk[Byte]](
                                32,
                                _ => ZIO.succeed(_ => true),
                                _ => ZIO.unit
                              )
                              .map(_.map(_._2))
                              .map(_.map(ZStream.fromQueue(_).unTake))
      msgOffSet <- Ref.make(0L).toManaged_
      ackMap    <- TMap.empty[Long, Promise[Nothing, Unit]].commit.toManaged_

      cluster = new InternalCluster(
        localMember,
        nodes,
        gossipState,
        userMessagesQueue,
        subscriberBroadcast,
        (c: Chunk[Byte]) => broadcastQueue.offer(c).unit,
        msgOffSet,
        ackMap
      )
      _ <- putStrLn("Beginning to accept connections").toManaged_
      _ <- cluster.acceptConnectionRequests.fork
      _ <- putStrLn("Connecting to seed nodes").toManaged_
      _ <- ZIO.foreach(seeds)(cluster.connect).toManaged_
      _ <- putStrLn("Starting SWIM membership protocol").toManaged_
      _ <- cluster.runSwim.fork.toManaged_
    } yield cluster
}
