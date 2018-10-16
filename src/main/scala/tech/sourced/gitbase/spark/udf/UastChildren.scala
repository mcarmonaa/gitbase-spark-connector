package tech.sourced.gitbase.spark.udf

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object UastChildren extends CustomUDF {
  /** Name of the function. */
  override def name: String = "uast_children"

  /** Function to execute when this function is called. */
  override def function: UserDefinedFunction = udf(get _)

  def get(marshaledNodes: Array[Byte]): Option[Array[Byte]] = {
    val nodes = BblfshUtils.unmarshalNodes(marshaledNodes).getOrElse(Seq.empty)
    val children = nodes.flatMap(_.children)
    BblfshUtils.marshalNodes(children)
  }
}
