name := "PEW"

sbtVersion := "1.2.3"

lazy val commonSettings = Seq (
	version := "0.2-SNAPSHOT",
	organization := "com.workflowfm",
	scalaVersion := "2.12.3"
)

// The dependencies are in Maven format, with % separating the parts.  
// Notice the extra bit "test" on the end of JUnit and ScalaTest, which will 
// mean it is only a test dependency.
//
// The %% means that it will automatically add the specific Scala version to the dependency name.  
// For instance, this will actually download scalatest_2.9.2

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0-SNAP10" % "test"
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.14.0" % "test"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4.12"
libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.5.16" % "test"
    
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.2.1"

libraryDependencies += "junit" % "junit" % "4.8.2"

EclipseKeys.preTasks := Seq(compile in Compile, compile in Test)

lazy val skiexample = project 
	.in(file("skiexample"))
	.settings(
        	commonSettings,
        	scalaSource in Compile := baseDirectory.value / "src",
        	scalaSource in Test := baseDirectory.value / "test"
	).dependsOn(rootRef)

lazy val root = project
	.in(file("."))
	.settings(
		commonSettings,
		scalaSource in Compile := baseDirectory.value / "src",
		scalaSource in Test := baseDirectory.value / "test"
	)
lazy val rootRef = LocalProject("root")
