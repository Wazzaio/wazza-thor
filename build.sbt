name := "ThoR"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++=  Seq(
  "org.apache.spark" %% "spark-core" % "1.1.0",
  "org.apache.hadoop" % "hadoop-client" % "2.2.0",
  "org.mongodb" % "mongo-java-driver" % "2.11.4",
  "com.typesafe.akka" %% "akka-actor" % "2.3.1",
  "com.typesafe.play" %% "play-json" % "2.2.1",
  "com.github.nscala-time" %% "nscala-time" % "1.2.0",
  "org.mongodb" %% "casbah" % "2.7.3",
  "com.typesafe" % "config" % "1.2.1",
  "com.github.nscala-time" %% "nscala-time" % "1.0.0",
  "org.scalaj" %% "scalaj-http" % "1.1.0",
  "org.quartz-scheduler" % "quartz" % "2.2.1"
)

fork := true

javaOptions in run += "-Xms1024M"

retrieveManaged := true

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "JCenter" at "http://jcenter.bintray.com/"

// For stable releases
resolvers += "Sonatype releases" at "https://oss.sonatype.org/content/repositories/releases"

mainClass in assembly := Some("wazza.thor.JobRunner")

