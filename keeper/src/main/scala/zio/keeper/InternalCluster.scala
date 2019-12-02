package zio.keeper

import zio._
import zio.clock.Clock
import zio.console.{ Console, putStrLn }
import zio.duration._
import zio.keeper.Cluster.{ readMessage, serializeMessage }
import zio.keeper.ClusterError._
import zio.keeper.discovery.Discovery
import zio.keeper.protocol.InternalProtocol
import zio.keeper.protocol.InternalProtocol._
import zio.keeper.transport.{ ChannelOut, Transport }
import zio.nio.{ InetAddress, SocketAddress }
import zio.stm.{ STM, TMap }
import zio.stream.{ Stream, ZStream }

import scala.collection.immutable.SortedSet

final class InternalCluster(
  localMember: Member,
  nodeChannels: Ref[Map[NodeId, ChannelOut]],
  gossipStateRef: Ref[GossipState],
  userMessageQueue: zio.Queue[Message],
  subscribeToBroadcast: UIO[Stream[Nothing, Chunk[Byte]]],
  publishToBroadcast: Chunk[Byte] => UIO[Unit],
  msgOffset: Ref[Long],
  acks: TMap[Long, Promise[Nothing, Unit]]
) extends Cluster {

  private def removeMember(member: Member) =
    gossipStateRef.update(_.removeMember(member)) *>
      nodeChannels.update(_ - member.nodeId) *>
      putStrLn("remove member: " + member)

  private def addMember(member: Member, send: ChannelOut) =
    gossipStateRef.update(_.addMember(member)) *>
      nodeChannels.update(_ + (member.nodeId -> send)) *>
      putStrLn("add member: " + member)

  private def updateState(newState: GossipState) =
    for {
      current <- gossipStateRef.get
      diff    = newState.diff(current)
//      _       <- ZIO.foreach(diff.local)(n => n.addr.socketAddress >>= connect)
    } yield ()

  private def sendMessage(to: NodeId, msgType: Int, payload: Chunk[Byte]) =
    for {
      node <- nodeChannels.get.map(_.get(to))
      _ <- node match {
            case Some(channel) =>
              serializeMessage(localMember, payload, msgType) >>= channel.send
            case None => ZIO.fail(UnknownNode(to))
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
      bytes  <- readMessage(channel)
      msg    <- InternalProtocol.deserialize(bytes._2.payload)
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
    for {
      env          <- ZManaged.environment[Console with Transport with Clock]
      localAddress <- localMember.addr.socketAddress.toManaged_
      server <- transport.bind(localAddress) { channelOut =>
                 (for {
                   state <- gossipStateRef.get
                   _     <- sendInternalMessage(channelOut, NewConnection(state, localMember))
                   _ <- expects(channelOut) {
                         case JoinCluster(remoteState, remoteMember) =>
                           putStrLn(remoteMember + " joined cluster") *>
                             updateState(remoteState) *>
                             addMember(remoteMember, channelOut) *>
                             listenOnChannel(channelOut, remoteMember)
                       }
                 } yield ())
                   .catchAll(ex => putStrLn(s"Connection failed: $ex"))
                   .provide(env)
               }
    } yield server

  private def connectToSeeds(seeds: Set[SocketAddress]) =
    for {
      _            <- ZIO.foreach(seeds)(connect)
      currentNodes <- nodeChannels.get
      currentState <- gossipStateRef.get
      _ <- ZIO.foreach(currentNodes.values)(
            channel => sendInternalMessage(channel, JoinCluster(currentState, localMember))
          )
    } yield ()

  private def connect(
    addr: SocketAddress
  ) =
    for {
      connectionInit <- Promise.make[Error, (Member, ChannelOut)]
      _ <- transport
            .connect(addr)
            .use { channel =>
              putStrLn(s"Initiating handshake with node at ${addr}") *>
                expects(channel) {
                  case NewConnection(remoteState, remoteMember) =>
                    (for {
                      state    <- gossipStateRef.get
                      newState = state.merge(remoteState)
                      _        <- updateState(newState)
                      _        <- addMember(remoteMember, channel)
                    } yield ()) *> connectionInit.succeed((remoteMember, channel)) *> listenOnChannel(
                      channel,
                      remoteMember
                    )
                }
            }
            .mapError(HandshakeError(addr, _))
            .catchAll(ex => putStrLn(s"Failed initiating connection with node [ ${addr} ]: $ex"))
            .fork
      _ <- connectionInit.await
    } yield ()

  private def listenOnChannel(
    channel: ChannelOut,
    partner: Member
  ): ZIO[Transport with Console with Clock, Error, Unit] = {

    def handleSends(messages: Stream[Nothing, Chunk[Byte]]) =
      messages.tap { bytes =>
        channel
          .send(bytes)
          .catchAll(ex => ZIO.fail(SendError(partner.nodeId, bytes, ex)))
      }.runDrain

    (for {
      _                   <- putStrLn(s"Setting up connection with [ ${partner.nodeId} ]")
      clusterMessageQueue <- Queue.bounded[Message](1000)
      _                   <- routeMessages(channel, clusterMessageQueue, userMessageQueue).fork
      broadcasted         <- subscribeToBroadcast
      _                   <- handleSends(broadcasted).fork
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
        putStrLn(s"channel close because of: $ex") //*> channel.close
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
    nodeChannels.get
      .map(_.keys.toList)

  override def send(data: Chunk[Byte], receipt: NodeId): IO[Error, Unit] =
    sendMessage(receipt, 2, data)

  override def broadcast(data: Chunk[Byte]): IO[Error, Unit] =
    serializeMessage(localMember, data, 2).flatMap[Any, Error, Unit](publishToBroadcast).unit

  override def receive: Stream[Error, Message] =
    zio.stream.Stream.fromQueue(userMessageQueue)

}

object InternalCluster {

  private[keeper] def initCluster(port: Int) =
    for {
      localHost         <- InetAddress.localHost.toManaged_.orDie
      localMember       = Member(NodeId.generateNew, NodeAddress(localHost.address, port))
      _                 <- putStrLn(s"Starting node [ ${localMember.nodeId} ]").toManaged_
      nodes             <- zio.Ref.make(Map.empty[NodeId, ChannelOut]).toManaged_
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

      _ <- putStrLn("Connecting to seed nodes: " + seeds).toManaged_
      _ <- cluster.connectToSeeds(seeds).toManaged_
      _ <- putStrLn("Beginning to accept connections").toManaged_
      _ <- cluster.acceptConnectionRequests.use(channel => ZIO.never.ensuring(channel.close.ignore)).toManaged_.fork
      _ <- putStrLn("Starting SWIM membership protocol").toManaged_
      _ <- cluster.runSwim.fork.toManaged_
    } yield cluster
}
