//package zio.keeper
//import zio.ZIO
//import zio.nio.SocketAddress
//
//package object discovery extends Discovery.Service[Discovery] {
//
//  override def discoverNodes: ZIO[Discovery, Error, Set[SocketAddress]] =
//    ZIO.accessM(_.discover.discoverNodes)
//}
