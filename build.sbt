addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

ThisBuild / scalaVersion := "2.12.8"
ThisBuild / version := "0.1.0-SNAPSHOT"

lazy val root = project
  .in(file("."))
  .settings(
    name := "scalaz-ziokeeper",
    libraryDependencies += "org.scalaz" %% "scalaz-zio" % "0.16"
  )
