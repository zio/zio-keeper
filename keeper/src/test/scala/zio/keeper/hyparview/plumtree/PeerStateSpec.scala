// package zio.keeper.hyparview.plumtree

// import zio._
// import zio.keeper.KeeperSpec
// import zio.keeper.hyparview.testing.TestPeerService
// import zio.keeper.hyparview.{ PeerService, TRandom }
// import zio.keeper.transport.Transport
// import zio.keeper.transport.testing.TestTransport
// import zio.keeper.gens
// import zio.logging.Logging
// import zio.stm._
// import zio.test.Assertion._
// import zio.test._

// object PeerStateSpec extends KeeperSpec {

//   val spec = {

//     val environment =
//       (TestTransport.make() ++ Logging.ignore ++ TRandom.live) >>>
//         (ZLayer.identity[Logging with TRandom with Transport] ++ TestPeerService.make(address(0))) >>>
//         (ZLayer
//           .identity[Transport with Logging with TRandom with TestPeerService with PeerService] ++ PeerState
//           .live(2))

//     suite("PeerState")(
//       testM("takes initial eager peers from PeerService") {
//         checkM(Gen.listOf(gens.nodeAddress).map(_.toSet)) { peers =>
//           {
//             for {
//               _      <- TestPeerService.setPeers(peers).commit
//               result <- PeerState.live[Int](peers.size).build.use(_.get.eagerPushPeers.commit)
//             } yield assert(result)(hasSameElements(peers))
//           }.provideCustomLayer(environment)
//         }
//       },
//       testM("has lazy peers as an empty set initially") {
//         checkM(Gen.listOf(gens.nodeAddress).map(_.toSet)) { peers =>
//           {
//             for {
//               _      <- TestPeerService.setPeers(peers).commit
//               result <- PeerState.live[Int](peers.size).build.use(_.get.lazyPushPeers.commit)
//             } yield assert(result)(isEmpty)
//           }.provideCustomLayer(environment)
//         }

//       },
//       testM("remove removes from eager peers") {
//         assertM(
//           ZSTM.atomically {
//             for {
//               _     <- PeerState.addToEagerPeers(address(1))
//               _     <- PeerState.removePeer(address(1))
//               peers <- PeerState.eagerPushPeers
//             } yield peers
//           }
//         )(isEmpty).provideCustomLayer(environment)
//       },
//       testM("remove removes from lazy peers") {
//         assertM(
//           ZSTM.atomically {
//             for {
//               _     <- PeerState.addToEagerPeers(address(1))
//               _     <- PeerState.moveToLazyPeers(address(1))
//               _     <- PeerState.removePeer(address(1))
//               peers <- PeerState.lazyPushPeers
//             } yield peers
//           }
//         )(isEmpty).provideCustomLayer(environment)
//       },
//       testM("addToEagerPeers adds to eager peers") {
//         checkM(Gen.listOf(gens.nodeAddress).map(_.toSet)) { peers =>
//           assertM((ZSTM.foreach_(peers)(PeerState.addToEagerPeers) *> PeerState.eagerPushPeers).commit)(
//             hasSameElements(peers)
//           ).provideCustomLayer(environment)
//         }
//       },
//       testM("addToEagerPeers removes from lazy peers") {
//         assertM(
//           ZSTM.atomically {
//             for {
//               _     <- PeerState.addToEagerPeers(address(1))
//               _     <- PeerState.moveToLazyPeers(address(1))
//               _     <- PeerState.addToEagerPeers(address(1))
//               peers <- PeerState.lazyPushPeers
//             } yield peers
//           }
//         )(isEmpty).provideCustomLayer(environment)
//       }
//       // TODO: move to PlumTree tests
//       //          testM("sends prune message when moving to lazy peers") {
//       //            checkM(Gen.anyInt.filter(_ != 0)) {
//       //              peer =>
//       //                env() {
//       //                  for {
//       //                    f <- InMemoryTransport
//       //                          .asNode(peer) {
//       //                            TestPeerService
//       //                              .make(peer)
//       //                              .use(
//       //                                ps =>
//       //                                  ps.peerService.receive
//       //                                    .take(1)
//       //                                    .runHead
//       //                              )
//       //                          }
//       //                          .fork
//       //                    _      <- InMemoryTransport.awaitAvailable(peer)
//       //                    _      <- TestPeerService.setPeers(Set(peer))
//       //                    _      <- PeerState.using[Int](ps => ps.addToEagerPeers(peer).commit *> ps.moveToLazyPeers(peer))
//       //                    result <- f.join
//       //                  } yield assert(result, isSome(equalTo((0, ActiveProtocol.Prune))))
//       //                }
//       //            }
//       //          }
//     )
//   }
// }
