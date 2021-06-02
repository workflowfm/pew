package com.workflowfm.pew

import sbt._

object Dependencies {

  val scalaVer = "2.12.12"


  val scalaTest = "org.scalatest" %% "scalatest" % "3.2.0-SNAP10" % "test"
  val scalaMock = "org.scalamock" %% "scalamock" % "4.1.0" % "test"
  val scalaCheck = "org.scalacheck" %% "scalacheck" % "1.14.0" % "test"
  val junit = "junit" % "junit" % "4.8.2" % "test"

  val akkaActor = "com.typesafe.akka" %% "akka-actor" % "2.6.1"
  val akkaTestkit = "com.typesafe.akka" %% "akka-testkit" % "2.6.1" % "test"

  val apache = "org.apache.commons" % "commons-lang3" % "3.3.2"


  val subakka = "uk.ac.ed.inf" %% "subakka" % "1.0.0"
  val subakkaTests = "uk.ac.ed.inf" %% "subakka" % "1.0.0" % Test classifier "tests"

  val simulator = "com.workflowfm" %% "proter" % "0.7.3"

  val sortImports = "com.nequissimus" %% "sort-imports" % "0.5.4"

  val mongo = "org.mongodb.scala" %% "mongo-scala-driver" % "2.9.0"

  val akkaStreamKafka = "com.typesafe.akka" %% "akka-stream-kafka" % "1.1.0"
  val akkaStream = "com.typesafe.akka" %% "akka-stream"       % "2.6.1"
  val akkaHttp = "com.typesafe.akka" %% "akka-http"         % "10.1.11"
  val akkaHttpJackson = "de.heikoseeberger" %% "akka-http-jackson" % "1.27.0"
  val kafka = "org.apache.kafka"  %% "kafka"             % "1.1.0"
  val kafkaStreams = "org.apache.kafka"  %  "kafka-streams"     % "1.1.0"

  val kafkaAll: Seq[ModuleID] = Seq( 
    akkaStreamKafka, 
    akkaStream,   
    akkaHttp,
    akkaHttpJackson,
    kafka,
    kafkaStreams,
  )

  val common: Seq[ModuleID] = Seq(
    akkaActor,
    subakka,
  )

  val testAll: Seq[ModuleID] = Seq(
    scalaTest,
    scalaMock,
    scalaCheck,
    junit,
    akkaTestkit,
    subakkaTests,
  )
}


