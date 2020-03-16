package zio.keeper.discovery

import zio.{ IO, Layer, UIO, ZLayer }
import zio.duration.Duration
import zio.keeper.Error
import zio.logging.Logging
import zio.nio.core.{ InetAddress, SocketAddress }

object Discovery {

  trait Service {
    def discoverNodes: IO[Error, Set[SocketAddress]]
  }

  def staticList(addresses: Set[SocketAddress]): Layer[Nothing, Discovery] =
    ZLayer.succeed {
      new Service {
        final override val discoverNodes: UIO[Set[SocketAddress]] =
          UIO.succeed(addresses)
      }
    }

  /**
   * This discovery strategy uses K8 service headless service dns to find other members of the cluster.
   *
   * Headless service is a service of type ClusterIP with the clusterIP property set to None.
   *
   */
  def k8Dns(address: InetAddress, timeout: Duration, port: Int): ZLayer[Logging, Nothing, Discovery] =
    ZLayer.fromFunction { logging =>
      new K8DnsDiscovery {
        val log               = logging
        val serviceDns        = address
        val serviceDnsTimeout = timeout
        val servicePort       = port
      }
    }
}
