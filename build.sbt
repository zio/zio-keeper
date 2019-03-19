addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

inThisBuild(
  List(
    name := "scalaz-distributed",
    version := "0.1.0-SNAPSHOT"
  )
)

lazy val root = project
  .in(file("."))
  .settings(
    libraryDependencies += "org.scalaz" %% "scalaz-zio" % "1.0-RC1"
  )
