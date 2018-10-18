package tech.sourced.gitbase.spark.udf

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.bblfsh.client.BblfshClient
import org.apache.spark.sql.functions.udf

object UastXPath extends CustomUDF {
  /** Name of the function. */
  override def name: String = "uast_xpath"

  /** Function to execute when this function is called. */
  override def function: UserDefinedFunction = udf(get _)

  def get(marshaledNodes: Array[Byte], query: String): Option[Array[Byte]] = {
    val nodes = BblfshUtils.unmarshalNodes(marshaledNodes).getOrElse(Seq())
    val filtered = nodes.flatMap(BblfshClient.filter(_, query))
    BblfshUtils.marshalNodes(filtered)
  }
}
