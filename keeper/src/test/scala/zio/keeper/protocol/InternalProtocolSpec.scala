package zio.keeper.protocol

import zio.Chunk
import zio.keeper.SerializationError.DeserializationTypeError
import zio.keeper.membership.{ GossipState, Member, NodeAddress, NodeId }
import zio.test.Assertion._
import zio.test._

import scala.collection.immutable.SortedSet

object InternalProtocolSpec extends DefaultRunnableSpec {

  val memberGen =
    for {
      ip   <- Gen.listOfN(4)(Gen.anyByte)
      port <- Gen.anyInt
    } yield Member(NodeId.generateNew, NodeAddress(ip.toArray, port))

  val gossipStateGen =
    for {
      members <- Gen.listOf(memberGen)
    } yield GossipState(SortedSet(members: _*))

  def serializationTest[R, A <: InternalProtocol](gen: Gen[R, A]) =
    checkM(gen) { original =>
      for {
        bytes        <- original.serialize
        deserialized <- InternalProtocol.deserialize(bytes)
      } yield assert(original)(equalTo(deserialized))
    }

  def spec = suite("Internal Protocol Serialization")(
    testM("Ping read and write") {
      serializationTest {
        for {
          conversation <- Gen.anyLong
          gossipState  <- gossipStateGen
        } yield InternalProtocol.Ping(conversation, gossipState)
      }
    },
    testM("PingReq read and write") {
      serializationTest {
        for {
          conversation <- Gen.anyLong
          gossipState  <- gossipStateGen
          targetNode   <- memberGen
        } yield InternalProtocol.PingReq(targetNode, conversation, gossipState)
      }
    },
    testM("Ack read and write") {
      serializationTest {
        for {
          conversation <- Gen.anyLong
          gossipState  <- gossipStateGen
        } yield InternalProtocol.Ack(conversation, gossipState)
      }
    },
    testM("OpenConnection read and write") {
      serializationTest {
        for {
          gossipState <- gossipStateGen
          member      <- memberGen
        } yield InternalProtocol.NewConnection(gossipState, member)
      }
    },
    testM("JoinCluster read and write") {
      serializationTest {
        for {
          gossipState <- gossipStateGen
          member      <- memberGen
        } yield InternalProtocol.JoinCluster(gossipState, member)
      }
    },
    testM("Malformed bytes") {
      assertM(InternalProtocol.deserialize(Chunk.single(Byte.MaxValue)).either)(
        isLeft(
          equalTo(DeserializationTypeError[InternalProtocol](upickle.core.Abort("expected dictionary got int32")))
        )
      )
    }
  )
}
