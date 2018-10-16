package tech.sourced.gitbase.spark.udf

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * Custom named user defined function.
  */
abstract class CustomUDF {
  /** Name of the function. */
  def name: String

  /** Function to execute when this function is called. */
  def function: UserDefinedFunction

  def apply(exprs: Column*): Column = function(exprs: _*)
}
