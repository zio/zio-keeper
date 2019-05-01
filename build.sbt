addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

ThisBuild / scalaVersion := "2.12.8"

lazy val root = project
  .in(file("."))
  .settings(
    name := "scalaz-ziokeeper",
    libraryDependencies ++= Seq(
      "org.scalaz" %% "scalaz-zio"           % "0.16",
      "org.specs2" %% "specs2-core"          % "4.5.1" % Test,
      "org.specs2" %% "specs2-scalacheck"    % "4.5.1" % Test,
      "org.specs2" %% "specs2-matcher-extra" % "4.5.1" % Test
    )
  )
