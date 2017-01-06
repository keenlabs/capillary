resolvers ++= Seq(
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases"
)

// The Play plugin
addSbtPlugin("com.typesafe.play" % "sbt-plugin" % "2.4.7")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.6.0")
