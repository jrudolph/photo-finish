val scalaV = "2.12.6"
val akkaV = "2.5.14"
val akkaHttpV = "10.1.4"

val specs2V = "4.3.2"

lazy val root = Project("root", file("."))
  .aggregate(core, web, docs)

lazy val core = project
  .settings(basicSettings)
  .settings(
    libraryDependencies ++= Seq(
      "io.spray" %% "spray-json" % "1.3.4",
      "com.drewnoakes" % "metadata-extractor" % "2.11.0",
      "net.java.dev.jna" % "jna" % "4.5.2",

      "com.typesafe.akka" %% "akka-actor" % akkaV, // for ByteString
      "com.typesafe.akka" %% "akka-http" % akkaHttpV, // for DateTime

      "org.specs2" %% "specs2-core" % specs2V % "test",
    )
  )

lazy val web = project
  .settings(basicSettings)
  .settings(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % akkaV,
      "com.typesafe.akka" %% "akka-http" % akkaHttpV,
    )
  )
  .dependsOn(core)

lazy val docs = project
  .settings(
    paradoxMaterialTheme in Compile := {
      ParadoxMaterialTheme()
        // choose from https://jonas.github.io/paradox-material-theme/getting-started.html#changing-the-color-palette
        .withColor("light-green", "amber")
        // choose from https://jonas.github.io/paradox-material-theme/getting-started.html#adding-a-logo
        .withLogoIcon("cloud")
        .withCopyright("Copyleft © Johannes Rudolph")
        .withRepository(uri("https://github.com/jrudolph/xyz"))
        .withSocial(
          uri("https://github.com/jrudolph"),
          uri("https://twitter.com/virtualvoid")
        )
    },

    paradoxProperties ++= Map(
      "github.base_url" -> (paradoxMaterialTheme in Compile).value.properties.getOrElse("repo", "")
    )
  )
  .enablePlugins(ParadoxMaterialThemePlugin)

lazy val basicSettings = Seq(
  organization := "net.virtual-void",
  version := "0.1-SNAPSHOT",

  scalaVersion := scalaV,

  scalacOptions ++= Seq(
    "-deprecation",
    "-unchecked",
    "-feature",
    "-Xlint"
  ),

  libraryDependencies ++= Seq(
    "org.specs2" %% "specs2-core" % specs2V % "test",
  ),

  fork in run := true,
  javaOptions in run += "-Djna.library.path=/home/johannes/git/self/photo-finish",
)