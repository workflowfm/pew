lazy val commonSettings = Seq (
	organization := "com.workflowfm",
	scalaVersion := "2.12.12"
)

autoAPIMappings := true

/* Only invoked when you do `doc` in SBT */
scalacOptions in (Compile, doc) += "-groups"
scalacOptions in (Compile, doc) += "-diagrams"
scalacOptions in (Compile, doc) += "-diagrams-debug"

// The dependencies are in Maven format, with % separating the parts.  
// Notice the extra bit "test" on the end of JUnit and ScalaTest, which will 
// mean it is only a test dependency.
//
// The %% means that it will automatically add the specific Scala version to the dependency name.  
// For instance, this will actually download scalatest_2.9.2

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0-SNAP10" % "test"
libraryDependencies += "org.scalamock" %% "scalamock" % "4.1.0" % Test
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.14.0" % "test"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.6.1"
libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.6.1" % "test"

libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.3.2"

libraryDependencies += "com.typesafe.akka" %% "akka-stream-kafka" % "1.1.0"
libraryDependencies += "com.typesafe.akka" %% "akka-stream"       % "2.6.1"
libraryDependencies += "com.typesafe.akka" %% "akka-http"         % "10.1.11"
libraryDependencies += "de.heikoseeberger" %% "akka-http-jackson" % "1.27.0"
libraryDependencies += "org.apache.kafka"  %% "kafka"             % "1.1.0"
libraryDependencies += "org.apache.kafka"  %  "kafka-streams"     % "1.1.0"

libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.2.1"

libraryDependencies += "junit" % "junit" % "4.8.2"

libraryDependencies += "uk.ac.ed.inf" %% "subakka" % "0.1-SNAPSHOT"
libraryDependencies += "uk.ac.ed.inf" %% "subakka" % "0.1-SNAPSHOT" % Test classifier "tests"

lazy val skiexample = project
  .in(file("skiexample"))
  .settings(
    commonSettings,
    scalaSource in Compile := baseDirectory.value / "src",
    scalaSource in Test := baseDirectory.value / "test"
  ).dependsOn(rootRef)

lazy val simulator = project
  .in(file("simulator"))
  .settings(
    commonSettings,
    name := "pew-simulator",
    libraryDependencies += "com.workflowfm" %% "wfm-simulator" % "0.2.1"
  ).dependsOn(rootRef)


lazy val root = project
  .in(file("."))
  .settings(
	commonSettings,
    name := "pew",
	scalaSource in Compile := baseDirectory.value / "src",
	scalaSource in Test := baseDirectory.value / "test"
  )
lazy val rootRef = LocalProject("root")
