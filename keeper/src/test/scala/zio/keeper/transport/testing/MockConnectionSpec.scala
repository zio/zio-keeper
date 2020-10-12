package zio.keeper.transport.testing

import zio._
import zio.test._
import zio.test.Assertion._
import zio.keeper.KeeperSpec
import zio.keeper.transport.Protocol
import zio.keeper.transport.testing.MockConnection._

object MockConnectionSpec extends KeeperSpec {

  override def spec =
    suite("MockConnection")(
      testM("emits messages") {
        val makeConnection = emit(1)
        makeConnection.useTest { con =>
          assertM(Protocol.take(1).run(con))(isSome(hasSameElements(Chunk(1))))
        }
      },
      testM("awaits messages - correct value") {
        val makeConnection = emit(1) ++ await(equalTo(2))
        val protocol = Protocol.fromFunction[Any, Nothing, Int, Int, Unit] { i: Int =>
          (Chunk.single(i + 1), Left(()))
        }
        makeConnection.useTest { con =>
          assertM(protocol.run(con))(isSome(equalTo(())))
        }
      },
      testM("awaits messages - incorrect value") {
        val makeConnection = emit(1) ++ await(equalTo(2))
        val protocol = Protocol.fromFunction[Any, Nothing, Int, Int, Unit] { i: Int =>
          (Chunk.single(i), Left(()))
        }
        makeConnection
          .useTest { con =>
            assertM(protocol.run(con))(isNone)
          }
          .map(result => assert(result.failures)(isSome(anything)))
      },
      testM("composes scripts using `++`") {
        val makeConnection = emit(1) ++ await(equalTo(2)) ++ emit(3) ++ await(equalTo(4)) ++ emit(5)
        def protocol: Protocol[Any, Nothing, Int, Int, Int] =
          Protocol.fromFunction(
            i =>
              if (i > 4) (Chunk.empty, Left(i))
              else (Chunk.single(i + 1), Right(protocol))
          )
        makeConnection.useTest { con =>
          assertM(Protocol.run(con, protocol))(isSome(equalTo(5)))
        }
      },
      testM("repeats scripts") {
        val makeConnection = (emit(1)
          ++ await(equalTo(1))).repeat(3)
        def protocol: Protocol[Any, Nothing, Int, Int, Int] =
          Protocol.fold(0) {
            case (n, i) =>
              if (n >= 3) (Chunk.single(i), None)
              else (Chunk.single(i), Some(n + 1))
          }
        makeConnection.useTest { con =>
          assertM(Protocol.run(con, protocol))(isSome(equalTo(3)))
        }
      },
      testM("closes stream when protocol is done") {
        val makeConnection = emit(1)
        makeConnection.useTest { con =>
          assertM(Protocol.take(2).run(con))(isNone)
        }
      },
      testM("fails stream on fail") {
        val makeConnection = await(equalTo(2)) ++ fail("boom")
        makeConnection.useTest { con =>
          val test =
            for {
              _      <- con.send(2).ignore
              result <- con.receive.runCollect
            } yield result
          assertM(test.run)(fails(equalTo("boom")))
        }
      }
    )
}
