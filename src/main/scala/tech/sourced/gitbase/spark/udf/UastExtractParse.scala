package tech.sourced.gitbase.spark.udf

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.util.control.NonFatal

object UastExtractParse extends CustomUDF with Logging {
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  override def name: String = "uast_extract_parse"

  override def function: UserDefinedFunction = udf(extract _)

  def extract(jsonArray: Array[Byte]): Option[Seq[String]] = {
    jsonArray match {
      case a if a == null || a.isEmpty => None
      case a => try {
        Some(mapper.readValue(a, classOf[Seq[String]]))
      } catch {
        case NonFatal(e) =>
          log.warn("Error trying to parse info from json array", e)
          None
      }
    }
  }
}
