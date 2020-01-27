val scalaV = "2.13.1"
val akkaV = "2.6.1"
val akkaHttpV = "10.1.11"

val scalaTestV = "3.1.0"

lazy val root: Project = project.in(file("."))
  .aggregate(core, web, docs)

val rootRef = ProjectRef(file("."), "root")

lazy val core: Project = project
  .settings(basicSettings)
  .settings(
    libraryDependencies ++= Seq(
      "io.spray" %% "spray-json" % "1.3.5",
      "com.drewnoakes" % "metadata-extractor" % "2.13.0",
      "net.java.dev.jna" % "jna" % "5.5.0",

      "com.typesafe.akka" %% "akka-actor" % akkaV,
      "com.typesafe.akka" %% "akka-stream" % akkaV,
      "com.typesafe.akka" %% "akka-http" % akkaHttpV, // for DateTime
    ),

    baseDirectory in reStart := (baseDirectory in rootRef).value,
    javaOptions in run += s"-Djna.library.path=${(baseDirectory in rootRef).value.getAbsolutePath}",
    javaOptions in reStart += s"-Djna.library.path=${(baseDirectory in rootRef).value.getAbsolutePath}",
  )

lazy val web = project
  .settings(basicSettings)
  .settings(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % akkaV,
      "com.typesafe.akka" %% "akka-http" % akkaHttpV,
    ),

    // Fix broken watchSources support in play/twirl, https://github.com/playframework/twirl/issues/186
    // watch sources support
    watchSources +=
      WatchSource(
        (sourceDirectory in TwirlKeys.compileTemplates).value,
        "*.scala.*",
        (excludeFilter in Global).value
      )
  )
  .enablePlugins(SbtTwirl)
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
    "-language:postfixOps",
    "-Xlint"
  ),

  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % scalaTestV % "test",
  ),

  fork in run := true,
  javaOptions in run ++= Seq(
    "-Djna.library.path=/home/johannes/git/self/photo-finish",
    "-XX:+PreserveFramePointer",
    "-XX:+UnlockDiagnosticVMOptions",
    "-XX:+DebugNonSafepoints",
  )
)