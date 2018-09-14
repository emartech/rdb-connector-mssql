name := "rdb-connector-mssql"

version := "0.1"

scalaVersion := "2.12.3"

lazy val ItTest = config("it") extend Test

lazy val root = (project in file("."))
  .configs(ItTest)
  .settings(
    inConfig(ItTest)(Seq(Defaults.itSettings: _*))
  )


resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies ++= {
  val scalaTestV = "3.0.1"
  val slickV = "3.2.0"
  Seq(
    "com.github.emartech"     %  "rdb-connector-common" % "ed038bfb07",
    "com.typesafe.slick"      %% "slick"                % slickV,
    "com.typesafe.slick"      %% "slick-hikaricp"       % slickV,
    "org.scalatest"           %% "scalatest"            % scalaTestV   % Test,
    "com.typesafe.akka"       %% "akka-stream-testkit"  % "2.5.6"      % Test,
    "com.github.emartech"     %  "rdb-connector-test"   % "0140815d5c" % Test,
    "com.microsoft.sqlserver" %  "mssql-jdbc"           % "6.4.0.jre8",
    "com.typesafe.akka"       %% "akka-http-spray-json" % "10.0.7"     % Test,
    "org.mockito"             %  "mockito-core"         % "2.11.0"     % Test
  )
}
