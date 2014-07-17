name := "wazza-batch-jobs"

version := "1.0"

scalaVersion := "2.10.3"

libraryDependencies ++=  Seq(
  "org.apache.spark" %% "spark-core" % "0.9.0-incubating",
  "org.apache.hadoop" % "hadoop-client" % "2.2.0",
  "org.mongodb" % "mongo-java-driver" % "2.11.4",
  "com.typesafe.akka" %% "akka-actor" % "2.3.1",
  "com.typesafe.play" %% "play-json" % "2.2.1",
  "com.github.nscala-time" %% "nscala-time" % "1.2.0"
)

retrieveManaged := true

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

