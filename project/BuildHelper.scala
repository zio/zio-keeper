import sbt._
import sbt.Keys._

object BuildHelper {

  def stdSettings(prjName: String) = Seq(
    name := s"$prjName",
    crossScalaVersions := Seq(Scala212, Scala213),
    scalaVersion in ThisBuild := Scala212,
    scalacOptions := stdOptions ++ extraOptions(scalaVersion.value),
    incOptions ~= (_.withLogRecompileOnMacro(false))
  )

  private val Scala212 = "2.12.10"
  private val Scala213 = "2.13.1"

  private val stdOptions = Seq(
    "-encoding",
    "UTF-8",
    "-explaintypes",
    "-Yrangepos",
    "-feature",
    "-language:higherKinds",
    "-language:existentials",
    "-Xlint:_,-type-parameter-shadow",
    "-Xsource:2.13",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard",
    "-unchecked",
    "-deprecation",
    "-Xfatal-warnings"
  )

  private val stdOpts213 = Seq(
    "-Wunused:imports",
    "-Wvalue-discard",
    "-Wunused:patvars",
    "-Wunused:privates",
    "-Wunused:params",
    "-Wvalue-discard",
    "-Wdead-code"
  )

  private val stdOptsUpto212 = Seq(
    "-Xfuture",
    "-Ypartial-unification",
    "-Ywarn-nullary-override",
    "-Yno-adapted-args",
    "-Ywarn-infer-any",
    "-Ywarn-inaccessible",
    "-Ywarn-nullary-unit",
    "-Ywarn-unused-import"
  )

  private def extraOptions(scalaVersion: String) =
    CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, 13)) =>
        stdOpts213
      case Some((2, 12)) =>
        Seq(
          "-opt-warnings",
          "-Ywarn-extra-implicit",
          "-Ywarn-unused:_,imports",
          "-Ywarn-unused:imports",
          "-opt:l:inline",
          "-opt-inline-from:<source>"
        ) ++ stdOptsUpto212
      case _ =>
        Seq("-Xexperimental") ++ stdOptsUpto212
    }
}
