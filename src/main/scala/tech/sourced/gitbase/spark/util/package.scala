package tech.sourced.gitbase.spark

import java.util.Optional

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import tech.sourced.gitbase.spark.udf.BblfshUtils

package object util {

  implicit class ScalaOptional[T](opt: Optional[T]) {
    def getOrElse(default: => T): T = {
      if (opt.isPresent) opt.get() else default
    }
  }

}
