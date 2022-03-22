val scalaV = "2.13.8"
val akkaV = "2.6.19"
val akkaHttpV = "10.2.9"

val scalaTestV = "3.2.11"

lazy val root: Project = project.in(file("."))
  .aggregate(core, web, docs)

val rootRef = ProjectRef(file("."), "root")

lazy val process: Project = project
  .settings(basicSettings)
  .settings(
    libraryDependencies ++= Seq(
      "io.spray" %% "spray-json" % "1.3.6",
      "org.xerial" % "sqlite-jdbc" % "3.30.1",

      "com.typesafe.akka" %% "akka-actor" % akkaV,
      "com.typesafe.akka" %% "akka-stream" % akkaV,
      "com.typesafe.akka" %% "akka-http" % akkaHttpV, // for DateTime
    ),
  )

lazy val core: Project = project
  .dependsOn(process)
  .settings(basicSettings)
  .settings(
    libraryDependencies ++= Seq(
      "com.drewnoakes" % "metadata-extractor" % "2.16.0",
      "net.java.dev.jna" % "jna" % "5.10.0",
    ),

    run / javaOptions += s"-Djna.library.path=${(rootRef / baseDirectory).value.getAbsolutePath}",
    reStart / javaOptions += s"-Djna.library.path=${(rootRef / baseDirectory).value.getAbsolutePath}",
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
        (TwirlKeys.compileTemplates / sourceDirectory).value,
        "*.scala.*",
        (excludeFilter in Global).value
      ),

    buildInfoPackage := "net.virtualvoid.fotofinish.web",
    buildInfoKeys ++= Seq(
      "longProjectName" -> "Photo Finish"
    ),
  )
  .enablePlugins(SbtTwirl, BuildInfoPlugin)
  .dependsOn(core)

lazy val docs = project
  .settings(
    Compile / paradoxMaterialTheme := {
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
      "github.base_url" -> (Compile / paradoxMaterialTheme).value.properties.getOrElse("repo", "")
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

  run / fork := true,
  run / javaOptions ++= Seq(
    "-Djna.library.path=/home/johannes/git/self/photo-finish",
    "-XX:+PreserveFramePointer",
    "-XX:+UnlockDiagnosticVMOptions",
    "-XX:+DebugNonSafepoints",
  ),
  reStart / baseDirectory := (rootRef / baseDirectory).value,
)