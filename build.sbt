import java.nio.file.{Files, StandardCopyOption}

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

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

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

sonatypeProfileName := "tech.sourced"

// pom settings for sonatype
homepage := Some(url("https://github.com/src-d/gitbase-spark-connector"))
scmInfo := Some(ScmInfo(url("https://github.com/src-d/gitbase-spark-connector"),
  "git@github.com:src-d/gitbase-spark-connector.git"))
developers += Developer("ajnavarro",
  "Antonio Navarro",
  "antonio@sourced.tech",
  url("https://github.com/ajnavarro"))
developers += Developer("kuba--",
  "Kuba Podgorski",
  "kuba@sourced.tech",
  url("https://github.com/kuba--"))
developers += Developer("mcarmonaa",
  "Manuel Carmona",
  "manuel@sourced.tech",
  url("https://github.com/mcarmonaa"))
developers += Developer("jfontan",
  "Javier Fontán",
  "javi@sourced.tech",
  url("https://github.com/jfontan"))
developers += Developer("erizocosmico",
  "Miguel Molina",
  "miguel@sourced.tech",
  url("https://github.com/erizocosmico"))
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
pomIncludeRepository := (_ => false)

crossPaths := false
publishMavenStyle := true

val SONATYPE_USERNAME = scala.util.Properties.envOrElse("SONATYPE_USERNAME", "NOT_SET")
val SONATYPE_PASSWORD = scala.util.Properties.envOrElse("SONATYPE_PASSWORD", "NOT_SET")
credentials += Credentials(
  "Sonatype Nexus Repository Manager",
  "oss.sonatype.org",
  SONATYPE_USERNAME,
  SONATYPE_PASSWORD)

val SONATYPE_PASSPHRASE = scala.util.Properties.envOrElse("SONATYPE_PASSPHRASE", "not set")

useGpg := false
pgpSecretRing := baseDirectory.value / "project" / ".gnupg" / "secring.gpg"
pgpPublicRing := baseDirectory.value / "project" / ".gnupg" / "pubring.gpg"
pgpPassphrase := Some(SONATYPE_PASSPHRASE.toArray)

packageBin in Compile := {
  val file = (packageBin in Compile).value
  val dest = new java.io.File(file.getParent, s"${name.value}-${version.value}-slim.jar")
  Files.copy(
    new java.io.File(file.getAbsolutePath).toPath,
    dest.toPath,
    StandardCopyOption.REPLACE_EXISTING
  )
  Files.delete(file.toPath)
  dest
}

publishArtifact in (Compile, packageBin) := false

val packageSlim = taskKey[File]("package-slim")

packageSlim := (packageBin in Compile).value

addArtifact(Artifact("gitbase-spark-connector", "jar", "jar", "slim"), packageSlim)

assembly := {
  val file = assembly.value
  val dest = new java.io.File(file.getParent, s"${name.value}-uber.jar")
  Files.copy(
    new java.io.File(file.getAbsolutePath).toPath,
    dest.toPath,
    StandardCopyOption.REPLACE_EXISTING
  )
  file
}

assembly := assembly.dependsOn(packageBin in Compile).value

test in assembly := {}

addArtifact(artifact in(Compile, assembly), assembly)

isSnapshot := version.value endsWith "SNAPSHOT"

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) {
    Some("snapshots" at nexus + "content/repositories/snapshots")
  } else {
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
  }
}