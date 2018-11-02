package tech.sourced.gitbase

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import tech.sourced.gitbase.spark.udf.BblfshUtils

package object spark {

  implicit class GitbaseSessionBuilder(val builder: SparkSession.Builder) extends AnyVal {

    def registerGitbaseSource(server: String = "",
                              config: Map[String, String] = defaultConfig,
                              rules: Seq[Rule[LogicalPlan]] = rule.getAll
                             ): SparkSession.Builder = {

      val gsConfig = if (server.isEmpty) {
        config
      } else {
        val s = server.split(",").head.trim
        config + (DefaultSource.GitbaseUrlKey -> s)
      }

      builder
        .config(new SparkConf(false).setAll(gsConfig))
        .withExtensions(injectRules(rules))

      val ss = builder.getOrCreate()
      udf.registerUDFs(ss)
      createTempViews(ss)

      builder
    }

  }

  val defaultConfig = Map(
    BblfshUtils.hostKey ->
      scala.util.Properties.envOrElse("BBLFSH_HOST", BblfshUtils.defaultHost),
    BblfshUtils.portKey ->
      scala.util.Properties.envOrElse("BBLFSH_PORT", BblfshUtils.defaultPort.toString),
    DefaultSource.GitbaseUrlKey ->
      scala.util.Properties.envOrElse("GITBASE_SERVERS", "0.0.0.0:3306")
  )

  def injectRules(rules: Seq[Rule[LogicalPlan]]): SparkSessionExtensions => Unit = {
    extensions: SparkSessionExtensions =>
      rules.foreach(rule =>
        extensions.injectOptimizerRule(session => rule)
      )
  }

  def createTempViews(ss: SparkSession): Unit = {
    Sources.orderedSources.foreach(source =>
      ss.read
        .format(DefaultSource.Name)
        .option(DefaultSource.TableNameKey, source)
        .option(DefaultSource.GitbaseUrlKey, ss.conf.get(DefaultSource.GitbaseUrlKey))
        .load()
        .createOrReplaceTempView(source)
    )
  }
}
