package zio.keeper.membership

import zio._
import zio.clock.Clock
import zio.console.{ Console, putStrLn }
import zio.duration._
import zio.keeper.Message.{ readMessage, serializeMessage }
import zio.keeper.ClusterError._
import zio.keeper.Message
import zio.keeper.protocol.InternalProtocol
import zio.keeper.protocol.InternalProtocol._
import zio.keeper.transport.{ ChannelOut, Transport }
import zio.nio.{ InetAddress, SocketAddress }
import zio.stm.{ STM, TMap }
import zio.stream.{ Stream, ZStream }
import zio.keeper._
import zio.keeper.discovery.Discovery
import zio.random.Random
import zio.macros.delegate._

import scala.collection.immutable.SortedSet

final class SWIM(
  localMember_ : Member,
  nodeChannels: Ref[Map[NodeId, ChannelOut]],
  gossipStateRef: Ref[GossipState],
  userMessageQueue: zio.Queue[Message],
  clusterMessageQueue: zio.Queue[Message],
  clusterEventsQueue: zio.Queue[MembershipEvent],
  subscribeToBroadcast: UIO[Stream[Nothing, Chunk[Byte]]],
  publishToBroadcast: Chunk[Byte] => UIO[Unit],
  msgOffset: Ref[Long],
  acks: TMap[Long, Promise[Error, Unit]]
) extends Membership.Service[Any] {

  override val events: ZStream[Any, Error, MembershipEvent] =
    ZStream.fromQueue(clusterEventsQueue)

  override val localMember: ZIO[Any, Nothing, Member] =
    ZIO.succeed(localMember_)

  private def removeMember(member: Member) =
    gossipStateRef.update(_.removeMember(member)) *>
      nodeChannels.update(_ - member.nodeId) *>
      putStrLn("remove member: " + member)

  private def addMember(member: Member, send: ChannelOut) =
    gossipStateRef.update(_.addMember(member)) *>
      nodeChannels.update(_ + (member.nodeId -> send)) *>
      putStrLn("add member: " + member)

  private def updateState(newState: GossipState): ZIO[Transport with Console with Clock, Error, Unit] =
    for {
      current <- gossipStateRef.get
      diff    = newState.diff(current)
      _       <- ZIO.foreach(diff.local)(n => (n.addr.socketAddress >>= connect).ignore)
    } yield ()

  private def sendMessage(to: NodeId, msgType: Int, payload: Chunk[Byte]) =
    for {
      node <- nodeChannels.get.map(_.get(to))
      _ <- node match {
            case Some(channel) =>
              serializeMessage(localMember_, payload, msgType) >>= channel.send
            case None => ZIO.fail(UnknownNode(to))
          }
    } yield ()

  private def sendInternalMessage(to: ChannelOut, msg: InternalProtocol): ZIO[Console, Error, Unit] = {
    for {
      _       <- putStrLn(s"sending $msg")
      payload <- msg.serialize
      msg     <- serializeMessage(localMember_, payload, 1)
      _       <- to.send(msg)
    } yield ()
  }.catchAll { ex =>
    putStrLn(s"error during sending message: $ex")
  }

  private def sendInternalMessage(to: NodeId, msg: InternalProtocol): ZIO[Console, Error, Unit] =
    for {
      node <- nodeChannels.get.map(_.get(to))
      _ <- node match {
            case Some(channel) =>
              sendInternalMessage(channel, msg)
            case None => ZIO.fail(UnknownNode(to))
          }
    } yield ()

  private def sendInternalMessageWithAck(to: NodeId, timeout: Duration)(fn: Long => InternalProtocol) =
    for {
      offset <- msgOffset.update(_ + 1)
      prom   <- Promise.make[Error, Unit]
      _      <- acks.put(offset, prom).commit
      msg    = fn(offset)
      _      <- putStrLn(s"sending $msg with ack and timeout: ${timeout.asScala.toSeconds} seconds")
      _      <- sendInternalMessage(to, fn(offset))
      _ <- prom.await
            .ensuring(acks.delete(offset).commit)
            .timeoutFail(AckMessageFail(offset, msg, to))(timeout)
    } yield ()

  private def expects[R, A](
    channel: ChannelOut
  )(pf: PartialFunction[InternalProtocol, ZIO[R, Error, A]]): ZIO[R, Error, A] =
    for {
      bytes  <- readMessage(channel)
      msg    <- InternalProtocol.deserialize(bytes._2.payload)
      result <- pf.lift(msg).getOrElse(ZIO.fail(UnexpectedMessage(bytes._2)))
    } yield result

  private def ack(id: Long) =
    for {
      promOpt <- acks
                  .get(id)
                  .flatMap(
                    _.fold(STM.succeed[Option[Promise[Error, Unit]]](None))(prom => acks.delete(id).as(Some(prom)))
                  )
                  .commit
      _ <- promOpt.fold(ZIO.unit)(_.succeed(()).unit)
    } yield ()

  private def runSwim =
    Ref.make(0).flatMap { roundRobinOffset =>
      val loop = gossipStateRef.get.map(_.members.filterNot(_ == localMember_).toIndexedSeq).flatMap { nodes =>
        if (nodes.nonEmpty) {
          for {
            next   <- roundRobinOffset.update(old => if (old < nodes.size - 1) old + 1 else 0)
            state  <- gossipStateRef.get
            target = nodes(next) // todo: insert in random position and keep going in round robin version
            _ <- sendInternalMessageWithAck(target.nodeId, 10.seconds)(ackId => Ping(ackId, state))
                  .foldM(
                    _ => // attempt req messages
                    {
                      val nodesWithoutTarget = nodes.filter(_ != target)
                      for {
                        jumps <- ZIO.collectAll(
                                  List.fill(Math.min(3, nodesWithoutTarget.size))(
                                    zio.random.nextInt(nodesWithoutTarget.size).map(nodesWithoutTarget(_))
                                  )
                                )
                        pingReqs = jumps.map { jump =>
                          sendInternalMessageWithAck(jump.nodeId, 10.seconds)(
                            ackId => PingReq(target, ackId, state)
                          )
                        }

                        _ <- if (pingReqs.nonEmpty) {
                              ZIO
                                .raceAll(pingReqs.head, pingReqs.tail)
                                .foldM(
                                  _ => removeMember(target),
                                  _ =>
                                    putStrLn(
                                      s"SWIM: Successful ping req to [ ${target.nodeId} ] through [ ${jumps.map(_.nodeId).mkString(", ")} ]"
                                    )
                                )
                            } else {
                              removeMember(target)
                            }
                      } yield ()
                    },
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
      _            <- handleClusterMessages(ZStream.fromQueue(clusterMessageQueue)).fork.toManaged_
      localAddress <- localMember.flatMap(_.addr.socketAddress).toManaged_
      server <- transport.bind(localAddress) { channelOut =>
                 (for {
                   state <- gossipStateRef.get
                   _     <- sendInternalMessage(channelOut, NewConnection(state, localMember_))
                   _ <- expects(channelOut) {
                         case JoinCluster(remoteState, remoteMember) =>
                           putStrLn(remoteMember.toString + " joined cluster") *>
                             addMember(remoteMember, channelOut) *>
                             updateState(remoteState) *>
                             listenOnChannel(channelOut, remoteMember)
                       }
                 } yield ())
                   .catchAll(ex => putStrLn(s"Connection failed: $ex"))
                   .provide(env)
               }
    } yield server

  private def connectToSeeds(seeds: Set[SocketAddress]) =
    for {
      _            <- ZIO.foreach(seeds)(seed => connect(seed).ignore)
      currentNodes <- nodeChannels.get
      currentState <- gossipStateRef.get
      _ <- ZIO.foreach(currentNodes.values)(
            channel => sendInternalMessage(channel, JoinCluster(currentState, localMember_))
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
                      _        <- addMember(remoteMember, channel)
                      _        <- updateState(newState)
                    } yield ()) *> connectionInit.succeed((remoteMember, channel)) *> listenOnChannel(
                      channel,
                      remoteMember
                    )
                }
            }
            .mapError(HandshakeError(addr, _))
            .catchAll(
              ex => connectionInit.fail(ex) *> putStrLn(s"Failed initiating connection with node [ ${addr} ]: $ex")
            )
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
      _           <- putStrLn(s"Setting up connection with [ ${partner.nodeId} ]")
      broadcasted <- subscribeToBroadcast
      _           <- handleSends(broadcasted).fork
      _           <- routeMessages(channel, clusterMessageQueue, userMessageQueue)
    } yield ())
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
        putStrLn(s"read message error: $ex")
      }
    loop.repeat(Schedule.doWhileM(_ => channel.isOpen.catchAll[Any, Nothing, Boolean](_ => ZIO.succeed(false))))
  }

  private def handleClusterMessages(stream: Stream[Nothing, Message]) =
    stream.tap { message =>
      for {
        payload <- InternalProtocol.deserialize(message.payload)
        _       <- putStrLn(s"receive message: $payload")
        _ <- payload match {
              case Ack(ackId, state) =>
                updateState(state) *>
                  ack(ackId)
              case Ping(ackId, state) =>
                for {
                  _     <- updateState(state)
                  state <- gossipStateRef.get
                  _     <- sendInternalMessage(message.sender, Ack(ackId, state))
                } yield ()
              case PingReq(target, originalAckId, state) =>
                for {
                  _     <- updateState(state)
                  state <- gossipStateRef.get
                  _ <- sendInternalMessageWithAck(target.nodeId, 10.seconds)(ackId => Ping(ackId, state))
                        .foldM(
                          _ => ZIO.unit,
                          _ =>
                            gossipStateRef.get
                              .flatMap(state => sendInternalMessage(message.sender, Ack(originalAckId, state)))
                        )
                        .fork
                } yield ()
              case _ => putStrLn("unknown message: " + payload)
            }
      } yield ()
    }.runDrain

  override def nodes: ZIO[Any, Nothing, List[NodeId]] =
    nodeChannels.get
      .map(_.keys.toList)

  override def send(data: Chunk[Byte], receipt: NodeId): IO[Error, Unit] =
    sendMessage(receipt, 2, data)

  override def broadcast(data: Chunk[Byte]): IO[Error, Unit] =
    serializeMessage(localMember_, data, 2).flatMap[Any, Error, Unit](publishToBroadcast).unit

  override def receive: Stream[Error, Message] =
    zio.stream.Stream.fromQueue(userMessageQueue)

}

object SWIM {

  def withSWIM(port: Int) = enrichWithManaged[Membership](join(port))

  def join(port: Int): ZManaged[Console with Clock with Random with Transport with Discovery, Error, Membership] =
    for {
      localHost            <- InetAddress.localHost.toManaged_.orDie
      localMember          = Member(NodeId.generateNew, NodeAddress(localHost.address, port))
      _                    <- putStrLn(s"Starting node [ ${localMember.nodeId} ]").toManaged_
      nodes                <- zio.Ref.make(Map.empty[NodeId, ChannelOut]).toManaged_
      seeds                <- discovery.discoverNodes.toManaged_
      _                    <- putStrLn("seeds: " + seeds).toManaged_
      userMessagesQueue    <- ZManaged.make(zio.Queue.bounded[Message](1000))(_.shutdown)
      clusterEventsQueue   <- ZManaged.make(zio.Queue.sliding[MembershipEvent](100))(_.shutdown)
      clusterMessagesQueue <- ZManaged.make(zio.Queue.bounded[Message](1000))(_.shutdown)
      gossipState          <- Ref.make(GossipState(SortedSet(localMember))).toManaged_
      broadcastQueue       <- ZManaged.make(zio.Queue.bounded[Chunk[Byte]](1000))(_.shutdown)
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
      ackMap    <- TMap.empty[Long, Promise[Error, Unit]].commit.toManaged_

      swimMembership = new SWIM(
        localMember_ = localMember,
        nodeChannels = nodes,
        gossipStateRef = gossipState,
        userMessageQueue = userMessagesQueue,
        clusterMessageQueue = clusterMessagesQueue,
        clusterEventsQueue = clusterEventsQueue,
        subscribeToBroadcast = subscriberBroadcast,
        publishToBroadcast = (c: Chunk[Byte]) => broadcastQueue.offer(c).unit,
        msgOffset = msgOffSet,
        acks = ackMap
      )

      _ <- putStrLn("Connecting to seed nodes: " + seeds).toManaged_
      _ <- swimMembership.connectToSeeds(seeds).toManaged_
      _ <- putStrLn("Beginning to accept connections").toManaged_
      _ <- swimMembership.acceptConnectionRequests
            .use(channel => ZIO.never.ensuring(channel.close.ignore))
            .toManaged_
            .fork
      _ <- putStrLn("Starting SWIM membership protocol").toManaged_
      _ <- swimMembership.runSwim.fork.toManaged_
    } yield new Membership {
      override def membership: Membership.Service[Any] = swimMembership
    }

}
