package zio.keeper.transport.testing

import zio._
import zio.test._
import zio.test.Assertion._
import zio.keeper.KeeperSpec
import zio.keeper.transport.Protocol

object ProtocolSpec extends KeeperSpec {

  override def spec =
    suite("MockConnection")(
      testM("emits messages") {
        val makeConnection = {
          import MockConnection._
          make(emit(1))
        }
        makeConnection.use { con =>
          assertM(Protocol.take(1).run(con))(isSome(hasSameElements(Chunk(1))))
        }
      },
      testM("awaits messages - correct value") {
        val makeConnection = {
          import MockConnection._
          make(emit(1) ++ await(equalTo(2)))
        }
        val protocol = Protocol.fromFunction[Any, Nothing, Int, Int, Unit] { i: Int =>
          (Chunk.single(i + 1), Left(()))
        }
        makeConnection.use { con =>
          assertM(protocol.run(con))(isSome(equalTo(())))
        }
      },
      testM("awaits messages - incorrect value") {
        val makeConnection = {
          import MockConnection._
          make(emit(1) ++ await(equalTo(2)))
        }
        val protocol = Protocol.fromFunction[Any, Nothing, Int, Int, Unit] { i: Int =>
          (Chunk.single(i), Left(()))
        }
        makeConnection.use { con =>
          assertM(protocol.run(con).run)(fails(anything))
        }
      },
      testM("composes scripts using `++`") {
        val makeConnection = {
          import MockConnection._
          make(
            emit(1)
              ++ await(equalTo(2))
              ++ emit(3)
              ++ await(equalTo(4))
              ++ emit(5)
          )
        }
        def protocol: Protocol[Any, Nothing, Int, Int, Int] =
          Protocol.fromFunction(
            i =>
              if (i > 4) (Chunk.empty, Left(i))
              else (Chunk.single(i + 1), Right(protocol))
          )
        makeConnection.use { con =>
          assertM(Protocol.run(con, protocol))(isSome(equalTo(5)))
        }
      },
      testM("composes scripts using `<|>`") {
        val makeConnection = {
          import MockConnection._
          make(
            emit(1)
              ++ ((await(equalTo(3)) ++ emit(4))
                <|> (await(equalTo(2)) ++ emit(3)))
          )
        }
        def protocol: Protocol[Any, Nothing, Int, Int, Int] =
          Protocol.fromFunction(
            i =>
              if (i >= 3) (Chunk.empty, Left(i))
              else (Chunk.single(i + 1), Right(protocol))
          )
        makeConnection.use { con =>
          assertM(Protocol.run(con, protocol))(isSome(equalTo(3)))
        }
      },
      testM("repeats scripts") {
        val makeConnection = {
          import MockConnection._
          make(
            (emit(1)
              ++ await(equalTo(1))).repeat(3)
          )
        }
        def protocol: Protocol[Any, Nothing, Int, Int, Int] =
          Protocol.fold(0) {
            case (n, i) =>
              if (n >= 3) (Chunk.single(i), None)
              else (Chunk.single(i), Some(n + 1))
          }
        makeConnection.use { con =>
          assertM(Protocol.run(con, protocol))(isSome(equalTo(3)))
        }
      },
      testM("closes stream when protocol is done") {
        val makeConnection = {
          import MockConnection._
          make(emit(1))
        }
        makeConnection.use { con =>
          assertM(Protocol.take(2).run(con))(isNone)
        }
      },
      testM("fails stream on fail") {
        val makeConnection = {
          import MockConnection._
          make(await(equalTo(1)))
        }
        makeConnection.use { con =>
          val test =
            for {
              _      <- con.send(2).ignore
              result <- con.receive.runCollect
            } yield result
          assertM(test.run)(fails(anything))
        }
      }
    )
}
