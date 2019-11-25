import BuildHelper._

inThisBuild(
  List(
    organization := "dev.zio",
    homepage := Some(url("https://zio.github.io/zio-keeper/")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer("jdegoes", "John De Goes", "john@degoes.net", url("http://degoes.net")),
      Developer("mijicd", "Dejan Mijic", "dmijic@acm.org", url("https://github.com/mijicd")),
      Developer(
        "pshemass",
        "Przemyslaw Wierzbicki",
        "rzbikson@gmail.com",
        url("https://github.com/pshemass")
      ),
      Developer(
        "mschuwalow",
        "Maxim Schuwalow",
        "maxim.schuwalow@gmail.com",
        url("https://github.com/mschuwalow")
      )
    ),
    pgpPassphrase := sys.env.get("PGP_PASSWORD").map(_.toArray),
    pgpPublicRing := file("/tmp/public.asc"),
    pgpSecretRing := file("/tmp/secret.asc"),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/zio/zio-keeper/"),
        "scm:git:git@github.com:zio/zio-keeper.git"
      )
    )
  )
)

ThisBuild / publishTo := sonatypePublishToBundle.value

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

lazy val root = project
  .in(file("."))
  .settings(skip in publish := true)
  .aggregate(keeper, membership, examples)

lazy val keeper = project
  .in(file("keeper"))
  .settings(stdSettings("zio-keeper"))
  .settings(
    libraryDependencies ++= Seq(
      "dev.zio"                %% "zio"                     % "1.0.0-RC17",
      "dev.zio"                %% "zio-streams"             % "1.0.0-RC17",
      "dev.zio"                %% "zio-nio"                 % "0.4.0",
      "dev.zio"                %% "zio-macros-core"         % "0.6.0",
      "com.lihaoyi"            %% "upickle"                 % "0.8.0",
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.2",
      "dev.zio"                %% "zio-test"                % "1.0.0-RC17" % Test,
      "dev.zio"                %% "zio-test-sbt"            % "1.0.0-RC17" % Test
    ),
    testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
  )

lazy val membership = project
  .in(file("membership"))
  .settings(stdSettings("zio-membership"))
  .settings(
    libraryDependencies ++= Seq(
      "dev.zio"                %% "zio"                     % "1.0.0-RC17",
      "dev.zio"                %% "zio-streams"             % "1.0.0-RC17",
      "dev.zio"                %% "zio-nio"                 % "0.4.0",
      "dev.zio"                %% "zio-macros-core"         % "0.6.0",
      "com.lihaoyi"            %% "upickle"                 % "0.8.0",
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.2",
      "dev.zio"                %% "zio-test"                % "1.0.0-RC17" % Test,
      "dev.zio"                %% "zio-test-sbt"            % "1.0.0-RC17" % Test
    ),
    testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
  )

lazy val examples = project
  .in(file("examples"))
  .settings(stdSettings("zio-keeper-examples"))
  .dependsOn(keeper)
  .settings(
    libraryDependencies ++= Seq(
      )
  )
