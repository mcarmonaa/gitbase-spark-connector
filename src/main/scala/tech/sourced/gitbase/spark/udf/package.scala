package tech.sourced.gitbase.spark

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.util.control.NonFatal

package object udf {
  private[udf] var spark: SparkSession = _

  private val gitbaseUdfs = Seq(
    Language,
    Uast,
    UastMode,
    UastXPath,
    UastExtract,
    UastChildren,
    IsBinary
  )

  private val udfs = Seq(
    UastExtractParse,
    ParseCommitParents
  )


  def isSupported(name: String): Boolean = gitbaseUdfs.exists(f => f.name == name)

  def registerUDFs(ss: SparkSession): Unit = {
    spark = ss
    gitbaseUdfs.foreach(f => spark.udf.register(f.name, f.function.withName(f.name)))
    udfs.foreach(f => spark.udf.register(f.name, f.function.withName(f.name)))
  }

  private[udf] object JsonArrayParser extends Logging {
    private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

    def extract(jsonArray: Array[Byte]): Option[Seq[String]] = {
      jsonArray match {
        case a if a == null || a.isEmpty => None
        case a => try {
          Option(mapper.readValue(a, classOf[Seq[String]]))
        } catch {
          case NonFatal(e) =>
            log.warn("Error trying to parse info from json array", e)
            None
        }
      }
    }
  }


}
