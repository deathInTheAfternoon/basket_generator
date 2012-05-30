name := "Basket Generator"

version := "0.1"

scalaVersion := "2.9.2"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "com.typesafe.akka" % "akka-actor" % "2.0.1"

libraryDependencies += "com.rabbitmq" % "amqp-client" % "2.8.2"

libraryDependencies += "com.mongodb.casbah" % "casbah_2.9.0-1" % "2.1.5.0"

// Wrapper for Joda Time - I may remove this if I don't use it.
libraryDependencies += "org.scala-tools.time" % "time_2.9.1" % "0.5"

//--------------------------------Spring-AMQP
libraryDependencies += "com.bluelock" % "camel-spring-amqp" % "1.1.0"

//--------------------------------Avro
seq( sbtavro.SbtAvro.avroSettings : _*)

//Put source where IntelliJ can see it 'src/main/java' instead of 'targe/scr_managed'.
javaSource in sbtavro.SbtAvro.avroConfig <<= (sourceDirectory in Compile)(_ / "java")

// Package needed by avro generated Java file (if we're not using dynamic schema).
libraryDependencies += "org.apache.avro" % "avro" % "1.6.3"

//-------------------------------Akka-Camel
resolvers += "Typesafe Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/"

//this also transitively brings in akka-actor of the same version.
libraryDependencies += "com.typesafe.akka" % "akka-camel" % "2.1-20120529-002605"
