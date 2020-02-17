package zio.keeper.discovery

import java.net.UnknownHostException
import java.util

import javax.naming.directory.InitialDirContext
import javax.naming.{Context, NamingException}
import zio.duration.Duration
import zio.keeper.ServiceDiscoveryError
import zio.logging.Logging
import zio.macros.delegate._
import zio.nio.core.{InetAddress, InetSocketAddress, SocketAddress}
import zio.{IO, URIO, ZIO, keeper}

/**
 * This discovery strategy uses K8 service headless service dns to find other members of the cluster.
 *
 * Headless service is a service of type ClusterIP with the clusterIP property set to None.
 *
 */
trait K8DnsDiscovery extends Discovery.Service[Any] {

  final override val discoverNodes: ZIO[Any, keeper.Error, Set[InetSocketAddress]] = {
    for {
      addresses <- lookup(serviceDns, serviceDnsTimeout)
      nodes     <- ZIO.foreach(addresses)(addr => SocketAddress.inetSocketAddress(addr, servicePort))
    } yield nodes.toSet: Set[InetSocketAddress]
  }.catchAll(
    ex =>
      logging.error("discovery strategy " + this.getClass.getSimpleName + " failed.") *>
        ZIO.fail(ServiceDiscoveryError(ex.getMessage))
  )
  val logging: Logging.Service[Any, String]

  def lookup(serviceDns: InetAddress, serviceDnsTimeout: Duration) = {
    import scala.jdk.CollectionConverters._
    val env = new util.Hashtable[String, String]
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory")
    env.put(Context.PROVIDER_URL, "dns:")
    env.put("com.sun.jndi.dns.timeout.initial", serviceDnsTimeout.toMillis.toString)

    for {
      dirContext   <- ZIO.effect(new InitialDirContext(env)).refineToOrDie[NamingException]
      attributes   <- ZIO.effectTotal(dirContext.getAttributes(serviceDns.hostname, Array[String]("SRV")))
      srvAttribute = Option(attributes.get("srv")).toList.flatMap(_.getAll.asScala)
      addresses <- ZIO.foldLeft(srvAttribute)(Set.empty[InetAddress]) {
                    case (acc, address: String) =>
                      extractHost(address)
                        .flatMap(InetAddress.byName)
                        .map(acc + _)
                        .refineToOrDie[UnknownHostException]
                    case (acc, _) =>
                      ZIO.succeed(acc)
                  }
    } yield addresses
  }

  private def extractHost(server: String) =
    logging.debug("k8 dns on response: " + server) *>
      IO.effectTotal {
        val host = server.split(" ")(3)
        host.replaceAll("\\\\.$", "")
      }

  def serviceDns: InetAddress

  def serviceDnsTimeout: Duration

  def servicePort: Int

}

object K8DnsDiscovery {

  def withK8DnsDiscovery(
    addr: InetAddress,
    timeout: Duration,
    port: Int
  ) = enrichWithM[Discovery](k8DnsDiscovery(addr, timeout, port))

  def k8DnsDiscovery(
    addr: InetAddress,
    timeout: Duration,
    port: Int
  ): URIO[Logging[String], Discovery] =
    ZIO.access[Logging[String]](
      env =>
        new Discovery {

          override def discover: Discovery.Service[Any] = new K8DnsDiscovery {
            override val logging: Logging.Service[Any, String] = env.logging

            override def serviceDns: InetAddress = addr

            override def serviceDnsTimeout: Duration = timeout

            override def servicePort: Int = port
          }
        }
    )

}
