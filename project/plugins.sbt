addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.21")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "3.0.2")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.4")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.0")
addSbtPlugin("com.47deg" % "sbt-microsites" % "1.4.3")
addSbtPlugin("com.github.sbt" % "sbt-protobuf" % "0.7.2")
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.11"
