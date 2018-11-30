package tech.sourced.gitbase.spark

import org.apache.spark.sql.SparkSession

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
    UastExtractParse
  )


  def isSupported(name: String): Boolean = gitbaseUdfs.exists(f => f.name == name)

  def registerUDFs(ss: SparkSession): Unit = {
    spark = ss
    gitbaseUdfs.foreach(f => spark.udf.register(f.name, f.function.withName(f.name)))
    udfs.foreach(f => spark.udf.register(f.name, f.function.withName(f.name)))
  }
}
