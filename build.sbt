import org.scalafmt.sbt.ScalafmtPlugin.scalafmtConfigSettings

lazy val commonSettings = Seq(
  scalaVersion := "2.12.3",
  organization := "com.emarsys",
  scalafmtOnCompile := true
)

lazy val ItTest         = config("it") extend Test
lazy val itTestSettings = Defaults.itSettings ++ scalafmtConfigSettings

lazy val root = (project in file("."))
  .configs(ItTest)
  .settings(inConfig(ItTest)(itTestSettings): _*)
  .settings(commonSettings: _*)
  .settings(
    name := "rdb-connector-mssql",
    version := "0.1-SNAPSHOT",
    resolvers += "jitpack" at "https://jitpack.io",
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding",
      "UTF-8",
      "-unchecked",
      "-feature",
      "-language:implicitConversions",
      "-language:postfixOps",
      "-Ywarn-dead-code",
      "-Xlint"
    ),
    libraryDependencies ++= {
      val scalaTestV = "3.0.1"
      val slickV     = "3.2.0"
      Seq(
        "com.github.emartech"     % "rdb-connector-common"  % "0c7ca8a",
        "com.typesafe.slick"      %% "slick"                % slickV,
        "com.typesafe.slick"      %% "slick-hikaricp"       % slickV,
        "org.scalatest"           %% "scalatest"            % scalaTestV % Test,
        "com.typesafe.akka"       %% "akka-stream-testkit"  % "2.5.6" % Test,
        "com.github.emartech"     % "rdb-connector-test"    % "2edd501019" % Test,
        "com.microsoft.sqlserver" % "mssql-jdbc"            % "6.4.0.jre8",
        "com.typesafe.akka"       %% "akka-http-spray-json" % "10.0.7" % Test,
        "org.mockito"             % "mockito-core"          % "2.11.0" % Test
      )
    }
  )

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt it:scalafmt")
addCommandAlias("testAll", "; test ; it:test")
