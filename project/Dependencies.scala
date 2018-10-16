import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
  lazy val sparkSql = "org.apache.spark" % "spark-sql_2.11" % "2.3.1"
  lazy val mariaDB = "org.mariadb.jdbc" % "mariadb-java-client" % "2.3.0"
  lazy val enry = "tech.sourced" % "enry-java" % "1.6.3"
  lazy val bblfsh = "org.bblfsh" % "bblfsh-client" % "1.10.1"
  lazy val dockerJava = "com.github.docker-java" % "docker-java" % "3.0.14"
}
