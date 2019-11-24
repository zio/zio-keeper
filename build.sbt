inThisBuild(
  List(
    organization := "dev.zio",
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer("jdegoes", "John De Goes", "john@degoes.net", url("http://degoes.net")),
      Developer("mijicd", "Dejan Mijic", "dmijic@acm.org", url("https://github.com/mijicd")),
      Developer(
        "pshemass",
        "Przemyslaw Wierzbicki",
        "rzbikson@gmail.com",
        url("https://github.com/pshemass")
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

lazy val zioKeeper = project
  .in(file("."))
  .settings(
    name := "zio-keeper",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio"          % "1.0.0-RC16",
      "dev.zio" %% "zio-streams"  % "1.0.0-RC16",
      "dev.zio" %% "zio-nio"      % "0.1.2",
      "dev.zio" %% "zio-test"     % "1.0.0-RC16" % "test",
      "dev.zio" %% "zio-test-sbt" % "1.0.0-RC16" % "test"
    )
  )

testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
