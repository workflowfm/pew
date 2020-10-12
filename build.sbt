import com.workflowfm.pew.Dependencies

lazy val commonSettings = Seq (
	organization := "com.workflowfm",
	scalaVersion := Dependencies.scalaVer,
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
    scalacOptions += "-Ywarn-unused" // required by `RemoveUnused` rule
)

autoAPIMappings := true

/* Only invoked when you do `doc` in SBT */
scalacOptions in (Compile, doc) += "-groups"
scalacOptions in (Compile, doc) += "-diagrams"
scalacOptions in (Compile, doc) += "-diagrams-debug"

// The %% means that it will automatically add the specific Scala version to the dependency name.  
// For instance, this will actually download scalatest_2.9.2

ThisBuild / scalafixDependencies += Dependencies.sortImports

lazy val aggregatedProjects: Seq[ProjectReference] = List[ProjectReference](
  pew,
  pewMongo,
  pewKafka,
  pewSimulator,
  skiexample
)

def pewModule(name: String): Project = 
  Project(id = name, base = file(name))
    .settings(commonSettings)
    .settings(libraryDependencies ++= Dependencies.common)
    .settings(libraryDependencies ++= Dependencies.testAll)
    .dependsOn(pew % "compile->compile;test->test")

def pewExample(name: String): Project = 
  Project(id = name, base = file(name))
    .settings(commonSettings)
    .settings(libraryDependencies ++= Dependencies.common)
    .dependsOn(pew)

lazy val root = Project(id = "pew-root", base = file("."))
  .aggregate(aggregatedProjects: _*)

//lazy val rootRef = LocalProject("root")

lazy val pew = Project(id = "pew", base = file("pew"))
  .settings(commonSettings)
  .settings(libraryDependencies ++= Dependencies.common)
  .settings(libraryDependencies ++= Dependencies.testAll)
  .settings(libraryDependencies ++= Seq(
    Dependencies.apache,
    Dependencies.mongo,
  ))

lazy val pewMongo =  pewModule("pew-mongo")

lazy val pewKafka =  pewModule("pew-kafka")
  .settings(libraryDependencies ++= Dependencies.kafkaAll)

lazy val pewSimulator =  pewModule("pew-simulator")
  .settings(libraryDependencies += Dependencies.simulator)

lazy val skiexample = pewExample("skiexample")
