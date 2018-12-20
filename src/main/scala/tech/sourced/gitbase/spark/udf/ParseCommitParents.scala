package tech.sourced.gitbase.spark.udf

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object ParseCommitParents extends CustomUDF {
  override def name: String = "parse_commit_parents"
  override def function: UserDefinedFunction = udf(JsonArrayParser.extract _)
}
