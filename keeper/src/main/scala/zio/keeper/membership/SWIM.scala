package zio.keeper.membership

import java.util.UUID

import zio._
import zio.clock.Clock
import zio.duration._
import zio.keeper.ClusterError._
import zio.keeper.Message.{ readMessage, serializeMessage }
import zio.keeper.{ Message, _ }
import zio.keeper.discovery.Discovery
import zio.keeper.protocol.InternalProtocol
import zio.keeper.protocol.InternalProtocol._
import zio.keeper.transport.Channel.{ Bind, Connection }
import zio.keeper.transport.Transport
import zio.logging
import zio.logging.Logging
import zio.nio.core.{ InetAddress, SocketAddress }
import zio.random.Random
import zio.stm.{ STM, TMap }
import zio.stream.Stream

import scala.collection.immutable.SortedSet

object SWIM {

  def join(
    port: Int
  ): ZLayer[Logging with Clock with Random with Transport with Discovery, Error, Membership] =
    ZLayer.fromManaged {
      for {
        localHost            <- InetAddress.localHost.toManaged_.orDie
        localMember          = Member(NodeId.generateNew, NodeAddress(localHost.address, port))
        _                    <- logging.logInfo(s"Starting node [ ${localMember.nodeId} ]").toManaged_
        nodes                <- zio.Ref.make(Map.empty[NodeId, Connection]).toManaged_
        seeds                <- discovery.discoverNodes.toManaged_
        _                    <- logging.logInfo("seeds: " + seeds).toManaged_
        userMessagesQueue    <- ZManaged.make(zio.Queue.bounded[Message](1000))(_.shutdown)
        clusterEventsQueue   <- ZManaged.make(zio.Queue.sliding[MembershipEvent](100))(_.shutdown)
        clusterMessagesQueue <- ZManaged.make(zio.Queue.bounded[Message](1000))(_.shutdown)
        gossipState          <- Ref.make(GossipState(SortedSet(localMember))).toManaged_
        broadcastQueue       <- ZManaged.make(Queue.bounded[Chunk[Byte]](1000))(_.shutdown)
        subscriberBroadcast <- Stream
                                .fromQueue(broadcastQueue)
                                .distributedWithDynamic[Nothing, Chunk[Byte]](
                                  32,
                                  _ => ZIO.succeed(_ => true),
                                  _ => ZIO.unit
                                )
                                .map(_.map(_._2))
                                .map(_.map(Stream.fromQueue(_).unTake))
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

        _ <- logging.logInfo("Connecting to seed nodes: " + seeds).toManaged_
        _ <- swimMembership.connectToSeeds(seeds).toManaged_
        _ <- logging.logInfo("Beginning to accept connections").toManaged_
        _ <- swimMembership.acceptConnectionRequests
              .use(channel => ZIO.never.ensuring(channel.close.ignore))
              .toManaged_
              .fork
        _ <- logging.logInfo("Starting SWIM membership protocol").toManaged_
        _ <- swimMembership.runSwim.fork.toManaged_
      } yield swimMembership
    }
}

final private class SWIM(
                          localMember_ : Member,
                          nodeChannels: Ref[Map[NodeId, Connection]],
                          gossipStateRef: Ref[GossipState],
                          userMessageQueue: Queue[Message],
                          clusterMessageQueue: Queue[Message],
                          clusterEventsQueue: Queue[MembershipEvent],
                          subscribeToBroadcast: UIO[Stream[Nothing, Chunk[Byte]]],
                          publishToBroadcast: Chunk[Byte] => UIO[Unit],
                          msgOffset: Ref[Long],
                          acks: TMap[Long, Promise[Error, Unit]]
) extends Membership.Service {

  val events: Stream[Error, MembershipEvent] =
    Stream.fromQueue(clusterEventsQueue)

  val localMember: UIO[Member] =
    UIO.succeed(localMember_)

  def broadcast(data: Chunk[Byte]): IO[Error, Unit] =
    serializeMessage(UUID.randomUUID(), localMember_, data, 2).flatMap(publishToBroadcast)

  def nodes: UIO[List[NodeId]] =
    nodeChannels.get.map(_.keys.toList)

  def receive: Stream[Error, Message] =
    Stream.fromQueue(userMessageQueue)

  def send(data: Chunk[Byte], receipt: NodeId): IO[Error, Unit] =
    sendMessage(receipt, 2, data)

  private def sendMessage(to: NodeId, msgType: Int, payload: Chunk[Byte]): IO[Error, Unit] =
    for {
      node <- nodeChannels.get.map(_.get(to))
      _ <- node match {
            case Some(channel) =>
              serializeMessage(UUID.randomUUID(), localMember_, payload, msgType) >>= channel.send
            case None =>
              IO.fail(UnknownNode(to))
          }
    } yield ()

  private def acceptConnectionRequests: ZManaged[Logging with Transport with Clock, TransportError, Bind] =
    for {
      env          <- ZManaged.environment[Logging with Transport with Clock]
      _            <- handleClusterMessages(Stream.fromQueue(clusterMessageQueue)).fork.toManaged_
      localAddress <- localMember.flatMap(_.addr.socketAddress).toManaged_
      server <- transport.bind(localAddress) { channelOut =>
                 (for {
                   state <- gossipStateRef.get
                   _     <- sendInternalMessage(channelOut, UUID.randomUUID(), NewConnection(state, localMember_))
                   _ <- expects(channelOut) {
                         case JoinCluster(remoteState, remoteMember) =>
                           logging.logInfo(s"${remoteMember.toString} joined cluster") *>
                             addMember(remoteMember, channelOut) *>
                             updateState(remoteState) *>
                             listenOnChannel(channelOut, remoteMember)
                       }
                 } yield ())
                   .catchAll(ex => logging.logError(Cause.fail(s"Connection failed: $ex")))
                   .provide(env)
               }
    } yield server

  private def ack(id: Long): URIO[Logging, Unit] =
    for {
      _ <- logging.logInfo(s"message ack $id")
      promOpt <- acks
                  .get(id)
                  .flatMap(
                    _.fold(STM.succeed[Option[Promise[Error, Unit]]](None))(prom => acks.delete(id).as(Some(prom)))
                  )
                  .commit
      _ <- promOpt.fold(ZIO.unit)(_.succeed(()).unit)
    } yield ()

  private def addMember(member: Member, send: Connection): URIO[Logging, Unit] =
    gossipStateRef.update(_.addMember(member)) *>
      nodeChannels.update(_ + (member.nodeId -> send)) *>
      propagateEvent(MembershipEvent.Join(member)) *>
      logging.logInfo(s"add member: $member")

  private def connect(addr: SocketAddress): ZIO[Logging with Transport with Clock, Error, Unit] =
    for {
      connectionInit <- Promise.make[Error, (Member, Connection)]
      _ <- transport
            .connect(addr)
            .use { channel =>
              logging.logInfo(s"Initiating handshake with node at ${addr}") *>
                expects(channel) {
                  case NewConnection(remoteState, remoteMember) =>
                    (for {
                      state    <- gossipStateRef.get
                      newState = state.merge(remoteState)
                      _        <- addMember(remoteMember, channel)
                      _        <- updateState(newState)
                    } yield ()) *>
                      connectionInit.succeed((remoteMember, channel)) *>
                      listenOnChannel(channel, remoteMember)
                }
            }
            .mapError(HandshakeError(addr, _))
            .catchAll(
              ex =>
                connectionInit.fail(ex) *>
                  logging.logError(Cause.fail(s"Failed initiating connection with node [ ${addr} ]: $ex"))
            )
            .fork
      _ <- connectionInit.await
    } yield ()

  private def connectToSeeds(seeds: Set[SocketAddress]): ZIO[Logging with Transport with Clock, Error, Unit] =
    for {
      _            <- ZIO.foreach(seeds)(seed => connect(seed).ignore)
      currentNodes <- nodeChannels.get
      currentState <- gossipStateRef.get
      _ <- ZIO.foreach(currentNodes.values)(
            channel => sendInternalMessage(channel, UUID.randomUUID(), JoinCluster(currentState, localMember_))
          )
    } yield ()

  private def expects[R, A](
    channel: Connection
  )(pf: PartialFunction[InternalProtocol, ZIO[R, Error, A]]): ZIO[R, Error, A] =
    for {
      bytes  <- readMessage(channel)
      msg    <- InternalProtocol.deserialize(bytes._2.payload)
      result <- pf.lift(msg).getOrElse(IO.fail(UnexpectedMessage(bytes._2)))
    } yield result

  private def handleClusterMessages(stream: Stream[Nothing, Message]) =
    stream.tap { message =>
      (for {
        payload <- InternalProtocol.deserialize(message.payload)
        _       <- logging.logInfo(s"receive message: $payload")
        _ <- payload match {
              case Ack(ackId, state) =>
                updateState(state) *> ack(ackId)
              case Ping(ackId, state) =>
                for {
                  _     <- updateState(state)
                  state <- gossipStateRef.get
                  _     <- sendInternalMessage(message.sender, message.id, Ack(ackId, state))
                } yield ()
              case PingReq(target, originalAckId, state) =>
                for {
                  _     <- updateState(state)
                  state <- gossipStateRef.get
                  _ <- sendInternalMessageWithAck(target.nodeId, 5.seconds)(ackId => Ping(ackId, state))
                        .foldM(
                          _ => ZIO.unit,
                          _ =>
                            gossipStateRef.get
                              .flatMap(
                                state => sendInternalMessage(message.sender, message.id, Ack(originalAckId, state))
                              )
                        )
                        .fork
                } yield ()
              case _ => logging.logError(Cause.fail(s"unknown message: $payload"))
            }
      } yield ())
        .catchAll(
          ex =>
            // we should probably reconnect to the sender.
            logging.logError(Cause.fail(s"Exception $ex processing cluster message $message"))
        )
    }.runDrain

  private def listenOnChannel(
                               channel: Connection,
                               partner: Member
  ): ZIO[Logging with Transport with Clock, Error, Unit] = {

    def handleSends(messages: Stream[Nothing, Chunk[Byte]]): IO[Error, Unit] =
      messages.foreach { bytes =>
        channel.send(bytes).catchAll(ex => IO.fail(SendError(partner.nodeId, bytes, ex)))
      }

    (for {
      _           <- logging.logInfo(s"Setting up connection with [ ${partner.nodeId} ]")
      broadcasted <- subscribeToBroadcast
      _           <- handleSends(broadcasted).fork
      _           <- routeMessages(channel, clusterMessageQueue, userMessageQueue)
    } yield ())
  }

  private def routeMessages(
                             channel: Connection,
                             clusterMessageQueue: Queue[Message],
                             userMessageQueue: Queue[Message]
  ): URIO[Logging, Unit] = {
    val loop = readMessage(channel)
      .flatMap {
        case (1, msg) =>
          clusterMessageQueue.offer(msg).unit
        case (2, msg) =>
          userMessageQueue.offer(msg).unit
        case (msgType, _) =>
          // this should be dead letter
          logging.logError(Cause.fail(s"unsupported message type $msgType"))
      }
      .catchAll { ex =>
        logging.logError(Cause.fail(s"read message error: $ex"))
      }
    loop.repeat(Schedule.doWhileM(_ => channel.isOpen.catchAll(_ => UIO.succeed(false))))
  }

  private def runSwim =
    Ref.make(0).flatMap { roundRobinOffset =>
      val loop: ZIO[Logging with Transport with Clock with Random, Error, Unit] =
        gossipStateRef.get.map(_.members.filterNot(_ == localMember_).toIndexedSeq).flatMap { nodes =>
          if (nodes.nonEmpty) {
            for {
              next   <- roundRobinOffset.updateAndGet(old => if (old < nodes.size - 1) old + 1 else 0)
              state  <- gossipStateRef.get
              target = nodes(next) // todo: insert in random position and keep going in round robin version
              _ <- sendInternalMessageWithAck(target.nodeId, 10.seconds)(ackId => Ping(ackId, state))
                    .foldM(
                      _ => // attempt req messages
                      {
                        val nodesWithoutTarget = nodes.filter(_ != target)
                        for {
                          _ <- propagateEvent(MembershipEvent.Unreachable(target))
                          jumps <- ZIO.collectAll(
                                    List.fill(Math.min(3, nodesWithoutTarget.size))(
                                      zio.random.nextInt(nodesWithoutTarget.size).map(nodesWithoutTarget(_))
                                    )
                                  )
                          pingReqs = jumps.map { jump =>
                            sendInternalMessageWithAck(jump.nodeId, 5.seconds)(
                              ackId => PingReq(target, ackId, state)
                            )
                          }

                          _ <- if (pingReqs.nonEmpty) {
                                ZIO
                                  .raceAll(pingReqs.head, pingReqs.tail)
                                  .foldM(
                                    _ => removeMember(target),
                                    _ =>
                                      logging.logInfo(
                                        s"Successful ping req to [ ${target.nodeId} ] through [ ${jumps.map(_.nodeId).mkString(", ")} ]"
                                      )
                                  )
                              } else {
                                logging.logInfo(s"Ack failed timeout") *>
                                  removeMember(target)
                              }
                        } yield ()
                      },
                      _ => logging.logInfo(s"Successful ping to [ ${target.nodeId} ]")
                    )
            } yield ()
          } else {
            logging.logInfo("No nodes to spread gossip to")
          }
        }

      loop.repeat(Schedule.spaced(10.seconds))
    }

  private def removeMember(member: Member): URIO[Logging, Unit] =
    gossipStateRef.update(_.removeMember(member)) *>
      nodeChannels.modify(old => (old.get(member.nodeId), old - member.nodeId)).flatMap {
        case Some(channel) =>
          channel.close.ignore *>
            logging.logInfo(s"channel closed for member: $member")
        case None =>
          ZIO.unit
      } *>
      propagateEvent(MembershipEvent.Leave(member)) *>
      logging.logInfo(s"remove member: $member")

  private def propagateEvent(event: MembershipEvent): UIO[Boolean] =
    clusterEventsQueue.offer(event)

  private def sendInternalMessageWithAck(to: NodeId, timeout: Duration)(
    fn: Long => InternalProtocol
  ): ZIO[Logging with Transport with Clock, Error, Unit] =
    for {
      offset <- msgOffset.updateAndGet(_ + 1)
      prom   <- Promise.make[Error, Unit]
      _      <- acks.put(offset, prom).commit
      msg    = fn(offset)
      _      <- sendInternalMessage(to, UUID.randomUUID, fn(offset))
      _ <- prom.await
            .ensuring(acks.delete(offset).commit)
            .timeoutFail(AckMessageFail(offset, msg, to))(timeout)
    } yield ()

  private def sendInternalMessage(
    to: NodeId,
    correlationId: UUID,
    msg: InternalProtocol
  ): ZIO[Logging, Error, Unit] =
    for {
      node <- nodeChannels.get.map(_.get(to))
      _ <- node match {
            case Some(channel) =>
              sendInternalMessage(channel, correlationId, msg)
            case None => ZIO.fail(UnknownNode(to))
          }
    } yield ()

  private def sendInternalMessage(
                                   to: Connection,
                                   correlationId: UUID,
                                   msg: InternalProtocol
  ): ZIO[Logging, Error, Unit] = {
    for {
      _       <- logging.logInfo(s"sending $msg")
      payload <- msg.serialize
      msg     <- serializeMessage(correlationId, localMember_, payload, 1)
      _       <- to.send(msg)
    } yield ()
  }.catchAll { ex =>
    logging.logInfo(s"error during sending message: $ex")
  }

  private def updateState(newState: GossipState): ZIO[Logging with Transport with Clock, Error, Unit] =
    for {
      current <- gossipStateRef.get
      diff    = newState.diff(current)
      _       <- ZIO.foreach(diff.local)(n => (n.addr.socketAddress >>= connect).ignore)
    } yield ()
}
