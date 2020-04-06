name := "MongoToPostegres"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.mongodb" % "mongo-java-driver" % "3.6.1"
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.9"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.8.1"
libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "log4j" % "log4j" % "1.2.17"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

assemblyJarName in assembly := "MongoDBToPostgres.jar"
test in assembly := {}