package tech.sourced.gitbase.spark.udf

import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import tech.sourced.enry.Enry

object Language extends CustomUDF with Logging {
  /** Name of the function. */
  override val name: String = "language"

  /** Function to execute when this function is called. */
  override val function: UserDefinedFunction = udf(get _)

  /**
    * Get the language a file is written in for the given path and content.
    * @param path file's path
    * @param content file's content
    * @return `None` if no language could be guessed, Some[String] otherwise.
    */
  def get(path: String, content: Array[Byte]): Option[String] = try {
    val l = Enry.getLanguage(path, content)
    if (l.isEmpty) None else Some(l)
  } catch {
    case e@(_: RuntimeException | _: Exception) =>
      log.error(s"get language for file $path failed", e)
      None
  }
}
