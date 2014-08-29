name := """capillary"""

version := "1.1"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-feature")

//libraryDependencies ++= Seq(
//  cache,
//  ws
//)

libraryDependencies ++= Seq(
    "org.apache.kafka" % "kafka_2.10" % "0.8.1.1"
      exclude("javax.jms", "jms")
      exclude("com.sun.jdmk", "jmxtools")
      exclude("com.sun.jmx", "jmxri")
      exclude("org.slf4j", "slf4j-simple"),
  "nl.grons" %% "metrics-scala" % "3.0.4",
  "com.codahale.metrics" % "metrics-json" % "3.0.1",
  "com.codahale.metrics" % "metrics-jvm" % "3.0.1"
)

libraryDependencies += "org.apache.curator" % "curator-framework" % "2.6.0"

libraryDependencies += "org.apache.curator" % "curator-recipes" % "2.6.0"
