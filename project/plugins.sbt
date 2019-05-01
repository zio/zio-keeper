dependsOn(RootProject(uri("git:https://github.com/scalaz/scalaz-sbt.git")))

addSbtPlugin("com.dwijnand"  % "sbt-dynver"        % "3.0.0")
addSbtPlugin("ch.epfl.scala" % "sbt-release-early" % "2.1.1")
