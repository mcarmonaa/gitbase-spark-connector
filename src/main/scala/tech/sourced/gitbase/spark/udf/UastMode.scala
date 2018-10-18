package tech.sourced.gitbase.spark.udf

import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object UastMode extends CustomUDF with Logging {
  /** Name of the function. */
  override def name: String = "uast_mode"

  /** Function to execute when this function is called. */
  override def function: UserDefinedFunction = udf(get _)

  private val modes = Seq("annotated", "semantic", "native")

  def get(mode: String = "annotated",
          content: Array[Byte],
          lang: String = ""): Option[Array[Byte]] = {

    if (content == null || content.isEmpty) {
      None
    } else {
      if (modes.contains(mode)) {
        Uast.get(content, Option(lang).getOrElse(""))
      } else {
        log.error(s"wrong mode type ${mode} found in call to udf ${name}")
        None
      }
    }
  }
}
