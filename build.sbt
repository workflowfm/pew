import com.workflowfm.pew.Dependencies

enablePlugins(HugoPlugin)
enablePlugins(SiteScaladocPlugin)
enablePlugins(GhpagesPlugin)

inThisBuild(List(
  organization := "com.workflowfm",
  organizationName := "WorkflowFM",
  organizationHomepage := Some(url("http://www.workflowfm.com/")),
  homepage := Some(url("http://www.workflowfm.com/pew/")),
  description := "A persistent execution engine for pi-calculus workflows",
  licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),

  developers := List(
    Developer(
      id = "PetrosPapapa",
      name = "Petros Papapanagiotou",
      email = "petros@workflowfm.com",
      url = url("https://homepages.inf.ed.ac.uk/ppapapan/")
    )
  ),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/workflowfm/pew"),
      "scm:git:git@github.com:workflowfm/pew.git"
    )
  ),
  dynverSonatypeSnapshots := true,

  scalafixDependencies += Dependencies.sortImports,
  git.remoteRepo := scmInfo.value.get.connection.replace("scm:git:", "")
))

// Publish to Sonatype / Maven Central

publishTo in ThisBuild := sonatypePublishToBundle.value
pomIncludeRepository := { _ => false }
publishMavenStyle := true
sonatypeCredentialHost := "s01.oss.sonatype.org"
sonatypeRepository := "https://s01.oss.sonatype.org/service/local"

// Website generation with sbt-site

Hugo / sourceDirectory := file("docs")
baseURL in Hugo := uri("http://docs.workflowfm.com/pew")
//baseURL in Hugo := uri("./")
includeFilter in Hugo := ("*")

ghpagesNoJekyll := true
previewFixedPort := Some(9999)


lazy val commonSettings = Seq (
  scalaVersion := Dependencies.scalaVer,

  semanticdbEnabled := true,
  semanticdbVersion := scalafixSemanticdb.revision,

  scalacOptions += "-Ywarn-unused", // required by `RemoveUnused` rule
  autoAPIMappings := true,
  Compile / doc / scalacOptions ++= Seq("-groups", "-implicits", "-diagrams", "-diagrams-debug"),

)

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
  .settings(commonSettings)
  .settings( 
    publishArtifact := false,
    siteSubdirName in ScalaUnidoc := "api",
    addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), siteSubdirName in ScalaUnidoc)
  )
  .aggregate(aggregatedProjects: _*)
  .enablePlugins(ScalaUnidocPlugin)

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
  .settings(publishArtifact := false)

lazy val pewKafka =  pewModule("pew-kafka")
  .settings(libraryDependencies ++= Dependencies.kafkaAll)

lazy val pewSimulator =  pewModule("pew-simulator")
  .settings(libraryDependencies += Dependencies.simulator)

lazy val skiexample = pewExample("skiexample")
  .settings(publishArtifact := false)
