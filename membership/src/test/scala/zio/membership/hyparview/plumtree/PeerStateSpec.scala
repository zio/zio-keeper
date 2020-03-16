package zio.membership.hyparview.plumtree

import java.math.BigInteger

import zio._
import zio.test._
import zio.test.Assertion._
import zio.membership.KeeperSpec
import zio.membership.hyparview.{ PeerService, TRandom }
import zio.keeper.membership.ByteCodec
import upickle.default._
import zio.keeper.SerializationError.DeserializationTypeError
import zio.logging.Logging
import zio.membership.hyparview.testing.TestPeerService
import zio.membership.transport.Transport
import zio.membership.transport.testing.InMemoryTransport
import zio.stm._

object PeerStateSpec extends KeeperSpec {

  val spec = {
    implicit val intByteCodec: ByteCodec[Int] = ByteCodec.instance(
      c => ZIO.effect(new BigInteger(c.toArray).intValue()).mapError(e => DeserializationTypeError(cause = e))
    )(
      i => ZIO.effectTotal(Chunk.fromArray(BigInt(i).toByteArray))
    )

    val environment =
      (InMemoryTransport.make[Int]() ++ Logging.ignore ++ TRandom.live) >>>
        (ZLayer.identity[Logging with TRandom with Transport[Int]] ++ TestPeerService.make(0)) >>>
        (ZLayer
          .identity[Transport[Int] with Logging with TRandom with TestPeerService[Int] with PeerService[Int]] ++ PeerState
          .live(2))

    suite("PeerState")(
      testM("takes initial eager peers from PeerService") {
        checkM(Gen.listOf(Gen.anyInt).map(_.toSet)) { peers =>
          {
            for {
              _      <- TestPeerService.setPeers(peers).commit
              result <- PeerState.live[Int](peers.size).build.use(_.get[PeerState.Service[Int]].eagerPushPeers.commit)
            } yield assert(result)(hasSameElements(peers))
          }.provideCustomLayer(environment)
        }
      },
      testM("has lazy peers as an empty set initially") {
        checkM(Gen.listOf(Gen.anyInt).map(_.toSet)) { peers =>
          {
            for {
              _      <- TestPeerService.setPeers(peers).commit
              result <- PeerState.live[Int](peers.size).build.use(_.get[PeerState.Service[Int]].lazyPushPeers.commit)
            } yield assert(result)(isEmpty)
          }.provideCustomLayer(environment)
        }

      },
      testM("remove removes from eager peers") {
        assertM(
          ZSTM.atomically {
            for {
              _     <- PeerState.addToEagerPeers(1)
              _     <- PeerState.removePeer(1)
              peers <- PeerState.eagerPushPeers[Int]
            } yield peers
          }
        )(isEmpty).provideCustomLayer(environment)
      },
      testM("remove removes from lazy peers") {
        assertM(
          ZSTM.atomically {
            for {
              _     <- PeerState.addToEagerPeers(1)
              _     <- PeerState.moveToLazyPeers(1)
              _     <- PeerState.removePeer(1)
              peers <- PeerState.lazyPushPeers[Int]
            } yield peers
          }
        )(isEmpty).provideCustomLayer(environment)
      },
      testM("addToEagerPeers adds to eager peers") {
        checkM(Gen.listOf(Gen.anyInt).map(_.toSet)) { peers =>
          assertM((ZSTM.foreach(peers)(PeerState.addToEagerPeers) *> PeerState.eagerPushPeers[Int]).commit)(
            hasSameElements(peers)
          ).provideCustomLayer(environment)
        }
      },
      testM("addToEagerPeers removes from lazy peers") {
        assertM(
          ZSTM.atomically {
            for {
              _     <- PeerState.addToEagerPeers(1)
              _     <- PeerState.moveToLazyPeers(1)
              _     <- PeerState.addToEagerPeers(1)
              peers <- PeerState.lazyPushPeers[Int]
            } yield peers
          }
        )(isEmpty).provideCustomLayer(environment)
      }
      // TODO: move to PlumTree tests
      //          testM("sends prune message when moving to lazy peers") {
      //            checkM(Gen.anyInt.filter(_ != 0)) {
      //              peer =>
      //                env() {
      //                  for {
      //                    f <- InMemoryTransport
      //                          .asNode(peer) {
      //                            TestPeerService
      //                              .make(peer)
      //                              .use(
      //                                ps =>
      //                                  ps.peerService.receive
      //                                    .take(1)
      //                                    .runHead
      //                              )
      //                          }
      //                          .fork
      //                    _      <- InMemoryTransport.awaitAvailable(peer)
      //                    _      <- TestPeerService.setPeers(Set(peer))
      //                    _      <- PeerState.using[Int](ps => ps.addToEagerPeers(peer).commit *> ps.moveToLazyPeers(peer))
      //                    result <- f.join
      //                  } yield assert(result, isSome(equalTo((0, ActiveProtocol.Prune))))
      //                }
      //            }
      //          }
    )
  }
}
