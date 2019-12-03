package zio.membership

import zio._
// import zio.duration._
// import zio.macros.delegate._
// import zio.membership.transport.tcp

object Main extends zio.ManagedApp {

  override def run(args: List[String]): ZManaged[zio.ZEnv, Nothing, Int] =
    ZManaged.succeed(0)
}
