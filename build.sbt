import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "tech.sourced",
      scalaVersion := "2.11.12"
    )),
    name := "gitbase-spark-connector",

    libraryDependencies += sparkSql % Provided,
    libraryDependencies ++= Seq(
      scalaTest % Test,
      dockerJava % Test
    ),
    libraryDependencies ++= Seq(
      mariaDB % Compile,
      enry % Compile,
      bblfsh % Compile
    )
  )

// option to show full stack traces
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oF")
parallelExecution in Test := false

scalastyleFailOnError := true
git.useGitDescribe := true

// Shade everything but tech.sourced.gitbase.spark so the user does not have conflicts
assemblyShadeRules := Seq(
  ShadeRule.rename("com.google.common.**" ->
    "tech.sourced.gitbase.spark.shaded.com.google.common.@1").inAll,
  ShadeRule.rename("com.google.protobuf.**" ->
    "tech.sourced.gitbase.spark.shaded.com.google.protobuf.@1").inAll,
  ShadeRule.rename("io.netty.**" ->
    "tech.sourced.gitbase.spark.shaded.io.netty.@1").inAll
)

assemblyMergeStrategy in assembly := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

enablePlugins(GitVersioning)
enablePlugins(ScalastylePlugin)
