package zio.keeper.example

import upickle.default._
import zio.clock.Clock
import zio.console.Console
import zio.{ App, Chunk, Has, IO, Ref, UIO, ZIO, ZLayer, ZManaged, console, keeper }
import zio.keeper.consensus.{ Consensus, Coordinator, JGroups, ReceiverAdapter }
import zio.keeper.membership.{ ByteCodec, Membership, TaggedCodec }

import scala.collection.immutable.HashMap
import DistributedHashMap._
import zio.keeper.consensus.Coordinator.CoordinatorMsg
import zio.logging.Logging.Logging
import zio.logging

/**
 * Try typing `PUT account1 10000` or `REMOVE account1` in any console
 */

object HashMapRun1 extends App {

  def run(args: List[String]) = program(8887, Set(8888, 8889)).fold(_ => 1, _ => 0)

}

object HashMapRun2 extends App {

  def run(args: List[String]) = program(8888, Set(8887, 8889)).fold(_ => 1, _ => 0)

}

object HashMapRun3 extends App {

  def run(args: List[String]) = program(8889, Set(8887, 8888)).fold(_ => 1, _ => 0)

}

object DistributedHashMap {

  /**
   * User defined operations on our data structure
   */
  trait SimpleStringMap {
    def containsKey(key: String): UIO[Boolean]
    def get(key: String): IO[Unit, String]
    def put(key: String, value: String): IO[keeper.Error, Unit]
    def remove(key: String): IO[keeper.Error, Unit]
  }

  sealed trait HashMapOperation
  case class HashMapPut(key: String, value: String) extends HashMapOperation
  case class HashMapRemove(key: String)             extends HashMapOperation

  val putMapCodec = ByteCodec.fromReadWriter(macroRW[HashMapPut])
  val remMapCodec = ByteCodec.fromReadWriter(macroRW[HashMapRemove])

  implicit val hashMapOpCodec =
    TaggedCodec.instance[HashMapOperation](
      {
        case _: HashMapPut    => 31
        case _: HashMapRemove => 32
      }, {
        case 31 => putMapCodec.asInstanceOf[ByteCodec[HashMapOperation]]
        case 32 => remMapCodec.asInstanceOf[ByteCodec[HashMapOperation]]
      }
    )
  implicit val hashMapByteCodec = ByteCodec.fromReadWriter(readwriter[Map[String, String]])
  implicit val hashMapTagCodec  = TaggedCodec.instance[Map[String, String]](_ => 100, { case _ => hashMapByteCodec })

  //========== HERE ARE BUILDING BLOCKS ==========

  /**
   *  Our ReceiverAdapter implementation
   */
  val hashmapBuildBlocks: ZLayer[JGroups with Logging, Nothing, Has[ReceiverAdapter.Service with SimpleStringMap]] =
    ZLayer.fromEffect[JGroups with Logging, Nothing, ReceiverAdapter.Service with SimpleStringMap](for {
      mapRef <- Ref.make[Map[String, String]](HashMap.empty[String, String])
      jg     <- ZIO.access[JGroups](_.get[JGroups.Service])
      logger <- ZIO.environment[Logging]
    } yield new ReceiverAdapter.Service with SimpleStringMap {

      private val hashMapRef = mapRef

      override def receive(data: Chunk[Byte]): IO[keeper.Error, Unit] =
        TaggedCodec.read[HashMapOperation](data).flatMap {
          case HashMapPut(key, value) =>
            logging.log.info(s"@@HASHMAP@@ PUT ${(key, value).toString}").provide(logger) *>
              hashMapRef.update(_ + (key -> value))
          case HashMapRemove(key) =>
            logging.log.info(s"@@HASHMAP@@ REMOVE $key").provide(logger) *>
              hashMapRef.update(_ - key)
        }

      override def setState(data: Chunk[Byte]): IO[keeper.Error, Unit] =
        for {
          state <- TaggedCodec.read[Map[String, String]](data)
          _     <- logging.log.info(s"@@HASHMAP@@ SET STATE ${state}").provide(logger)
          _     <- hashMapRef.set(state)
        } yield ()

      override def getState: IO[keeper.Error, Chunk[Byte]] =
        for {
          state    <- hashMapRef.get
          _        <- logging.log.info(s"@@HASHMAP@@ GET STATE ${state}").provide(logger)
          rawState <- TaggedCodec.write[Map[String, String]](state)
        } yield rawState

      override def containsKey(key: String): UIO[Boolean] =
        hashMapRef.get.map(m => m.contains(key))

      override def get(key: String): IO[Unit, String] =
        hashMapRef.get.flatMap(m => IO.fromOption(m.get(key)))

      override def put(key: String, value: String): IO[keeper.Error, Unit] =
        for {
          _       <- hashMapRef.update(_.updated(key, value))
          payload <- TaggedCodec.write[HashMapOperation](HashMapPut(key, value))
          _       <- jg.send(payload)
        } yield ()

      override def remove(key: String): IO[keeper.Error, Unit] =
        for {
          _       <- hashMapRef.update(_.filterNot { case (k, _) => k == key })
          payload <- TaggedCodec.write[HashMapOperation](HashMapRemove(key))
          _       <- jg.send(payload)
        } yield ()
    })

  //====== THE REST OF BUILDING BLOCKS ===========

  val consensus: ZLayer[Membership[CoordinatorMsg] with Clock with Logging, keeper.Error, Consensus] = Coordinator.live

  val jgroups
    : ZLayer[Membership[CoordinatorMsg] with Clock with Logging, keeper.Error, JGroups] = consensus >>> JGroups.live

  val hashMapComponents: ZLayer[Membership[CoordinatorMsg] with Clock with Logging, keeper.Error, Has[
    ReceiverAdapter.Service with SimpleStringMap
  ]] = (jgroups ++ ZLayer.requires[Logging]) >>> hashmapBuildBlocks

  val layerTransformer
    : ZLayer[Has[ReceiverAdapter.Service with SimpleStringMap], keeper.Error, ReceiverAdapter with Has[
      SimpleStringMap
    ]] =
    ZLayer
      .fromServiceManyManaged[ReceiverAdapter.Service with SimpleStringMap, Any, keeper.Error, ReceiverAdapter with Has[
        SimpleStringMap
      ]] { service =>
        ZLayer.succeedMany(Has.allOf[ReceiverAdapter.Service, SimpleStringMap](service, service)).build
      }

  val transformedComponents = hashMapComponents >>> layerTransformer

  val hashMap
    : ZLayer[Membership[CoordinatorMsg] with Clock with Logging, keeper.Error, Has[SimpleStringMap]] = (transformedComponents ++ consensus ++ Clock.any ++ ZLayer
    .requires[Logging]) >>> JGroups
    .create[SimpleStringMap]

  def hashMapManaged(
    port: Int,
    ports: Set[Int]
  ): ZManaged[Any, Exception, ZLayer[Any, keeper.Error, Has[SimpleStringMap]]] =
    for {
      env     <- TestNode.environment[CoordinatorMsg](port, ports)
      fullEnv = (Console.live ++ Clock.live) >>> env
    } yield (Clock.live ++ fullEnv) >>> hashMap

  def cmd =
    ZIO.access[Has[SimpleStringMap]](_.get[SimpleStringMap]) >>= { hashMap =>
      (for {
        str     <- console.getStrLn
        split   = str.split(" ")
        command <- IO(split(0))
        _ <- if (command == "PUT") {
              IO((split(1), split(2))) >>= (kv => hashMap.put(kv._1, kv._2))
            } else if (command == "REMOVE") {
              IO(split(1)) >>= (key => hashMap.remove(key))
            } else {
              console.putStrLn("Command not recognized")
            }
      } yield ()).forever
    }

  def program(port: Int, ports: Set[Int]): ZIO[Any, Exception, Unit] =
    hashMapManaged(port, ports).use(
      r =>
        cmd
          .provideLayer(r ++ console.Console.live)
          .catchAll(e => console.putStrLn(e.toString).provideLayer(Console.live))
    )

}
