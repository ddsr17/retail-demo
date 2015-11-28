name := """Titan_Scala"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  jdbc,
  cache,
  ws,
  specs2 % Test
)

libraryDependencies ++= Seq(
  "com.thinkaurelius.titan" % "titan-cassandra" % "0.5.4",
  "com.tinkerpop.blueprints" % "blueprints-core" % "2.6.0",
  "org.apache.commons" % "commons-csv" % "1.2"
)

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.2.0"

libraryDependencies ++= Seq(
  "org.webjars" %% "webjars-play" % "2.4.0-1",
  "org.webjars" % "bootstrap" % "3.1.1-2"
)

libraryDependencies ++= Seq(
  ws
)

dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
)

resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"

// Play provides two styles of routers, one expects its actions to be injected, the
// other, legacy style, accesses its actions statically.
routesGenerator := InjectedRoutesGenerator
