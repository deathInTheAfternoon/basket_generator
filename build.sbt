name := "Basket Generator"

version := "0.1"

scalaVersion := "2.9.1"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "com.typesafe.akka" % "akka-actor" % "2.0.1"

libraryDependencies += "com.rabbitmq" % "amqp-client" % "2.8.2"

libraryDependencies += "com.mongodb.casbah" % "casbah_2.9.0-1" % "2.1.5.0"