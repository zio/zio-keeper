package zio.keeper.discovery

import java.net.UnknownHostException
import java.util

import javax.naming.directory.InitialDirContext
import javax.naming.{ Context, NamingException }
import zio.{ Cause, IO, UIO, URIO, ZIO }
import zio.duration.Duration
import zio.keeper.{ Error, ServiceDiscoveryError }
import zio.logging._
import zio.logging.Logging
import zio.logging.Logging.Logging
import zio.nio.core.{ InetAddress, InetSocketAddress, SocketAddress }

private trait K8DnsDiscovery extends Discovery.Service {

  val logging: Logging

  val serviceDns: InetAddress

  val serviceDnsTimeout: Duration

  val servicePort: Int

  final override val discoverNodes: IO[Error, Set[InetSocketAddress]] = {
    for {
      addresses <- lookup(serviceDns, serviceDnsTimeout)
      nodes     <- IO.foreach(addresses)(addr => SocketAddress.inetSocketAddress(addr, servicePort))
    } yield nodes.toSet
  }.catchAll { ex =>
      log.error("Error in discovery", Cause.fail(s"discovery strategy ${this.getClass.getSimpleName} failed.")) *>
        IO.fail(ServiceDiscoveryError(ex.getMessage))
    }
    .provide(logging)

  private def lookup(
    serviceDns: InetAddress,
    serviceDnsTimeout: Duration
  ): ZIO[Logging, Exception, Set[InetAddress]] = {
    import scala.jdk.CollectionConverters._

    val env = new util.Hashtable[String, String]
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory")
    env.put(Context.PROVIDER_URL, "dns:")
    env.put("com.sun.jndi.dns.timeout.initial", serviceDnsTimeout.toMillis.toString)

    for {
      dirContext   <- IO.effect(new InitialDirContext(env)).refineToOrDie[NamingException]
      attributes   <- UIO.effectTotal(dirContext.getAttributes(serviceDns.hostname, Array("SRV")))
      srvAttribute = Option(attributes.get("srv")).toList.flatMap(_.getAll.asScala)
      addresses <- ZIO.foldLeft(srvAttribute)(Set.empty[InetAddress]) {
                    case (acc, address: String) =>
                      extractHost(address)
                        .flatMap(InetAddress.byName)
                        .map(acc + _)
                        .refineToOrDie[UnknownHostException]
                    case (acc, _) =>
                      UIO.succeed(acc)
                  }
    } yield addresses
  }

  private def extractHost(server: String): URIO[Logging, String] =
    log.debug(s"k8 dns on response: $server").provide(logging) *>
      UIO.effectTotal {
        val host = server.split(" ")(3)
        host.replaceAll("\\\\.$", "")
      }
}
