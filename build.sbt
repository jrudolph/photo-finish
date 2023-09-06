val scalaV = "2.13.10"
val pekkoV = "1.0.1"
val pekkoHttpV = "1.0.0"

val scalaTestV = "3.2.15"

lazy val root: Project = project.in(file("."))
  .aggregate(process, core, web, docs)

val rootRef = ProjectRef(file("."), "root")

lazy val process: Project = project
  .settings(basicSettings)
  .settings(
    libraryDependencies ++= Seq(
      "io.spray" %% "spray-json" % "1.3.6",
      "org.xerial" % "sqlite-jdbc" % "3.40.0.0",

      "org.apache.pekko" %% "pekko-actor" % pekkoV,
      "org.apache.pekko" %% "pekko-stream" % pekkoV,
      "org.apache.pekko" %% "pekko-http" % pekkoHttpV, // for DateTime
    ),
  )

lazy val core: Project = project
  .dependsOn(process)
  .settings(basicSettings)
  .settings(
    libraryDependencies ++= Seq(
      "com.drewnoakes" % "metadata-extractor" % "2.18.0",
      "net.java.dev.jna" % "jna" % "5.12.1",
    ),

    run / javaOptions += s"-Djna.library.path=${(rootRef / baseDirectory).value.getAbsolutePath}",
    reStart / javaOptions += s"-Djna.library.path=${(rootRef / baseDirectory).value.getAbsolutePath}",
  )

lazy val web = project
  .settings(basicSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-stream" % pekkoV,
      "org.apache.pekko" %% "pekko-http" % pekkoHttpV,
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

  resolvers += "Apache Nexus Snapshots" at "https://repository.apache.org/content/repositories/snapshots/",
  resolvers += "Apache Nexus Staging" at "https://repository.apache.org/content/repositories/staging/",
)