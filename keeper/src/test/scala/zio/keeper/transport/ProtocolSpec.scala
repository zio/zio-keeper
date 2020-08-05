package zio.keeper.transport

import zio._
import zio.test._
import zio.test.Assertion._
import zio.keeper.KeeperSpec
import zio.keeper.transport.testing.MockConnection

object ProtocolSpec extends KeeperSpec {

  override def spec =
    suite("Protocol")(
      testM("specific example") {
        val makeConnection = {
          import MockConnection._
          make(
            emit(1),
            await(equalTo(2)),
            emit(3),
            await(equalTo(4)),
            emit(5)
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
      }
    )

}
