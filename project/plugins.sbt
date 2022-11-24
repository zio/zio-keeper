addSbtPlugin("ch.epfl.scala"    % "sbt-bloop"                 % "1.4.6")
addSbtPlugin("com.geirsson"     % "sbt-ci-release"            % "1.5.5")
addSbtPlugin("com.github.cb372" % "sbt-explicit-dependencies" % "0.2.16")
addSbtPlugin("org.scalameta"    % "sbt-scalafmt"              % "2.4.2")
addSbtPlugin("org.scalameta"    % "sbt-mdoc"                  % "2.2.24")
addSbtPlugin("dev.zio"          % "zio-sbt-website"           % "0.0.0+84-6fd7d64e-SNAPSHOT")

resolvers += Resolver.sonatypeRepo("public")
