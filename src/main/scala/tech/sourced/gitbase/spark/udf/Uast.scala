package tech.sourced.gitbase.spark.udf

import gopkg.in.bblfsh.sdk.v1.protocol.generated.Status
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.bblfsh.client.BblfshClient


object Uast extends CustomUDF with Logging {
  /** Name of the function. */
  override val name: String = "uast"

  /** Function to execute when this function is called. */
  override def function: UserDefinedFunction = udf(get _)

  def get(content: Array[Byte],
          lang: String = "",
          query: String = ""): Option[Array[Byte]] =
    try {
      if (content == null || content.isEmpty) {
        return None
      }

      if (!BblfshUtils.isSupportedLanguage(lang)) {
        return None
      }

      val res = BblfshUtils
        .getClient().parse("", new String(content, "UTF-8"), lang)

      if (res.status != Status.OK) {
        log.warn(s"couldn't get UAST : error ${res.status}: ${res.errors.mkString("; ")}")
        return None
      }

      val nodes = query match {
        case "" => Seq(res.uast.get)
        case q: String => {
          BblfshClient.filter(res.uast.get, q)
        }
      }

      BblfshUtils.marshalNodes(nodes)
    } catch {
      case e@(_: RuntimeException | _: Exception) =>
        log.error(s"couldn't get UAST: $e")
        None
    }
}
