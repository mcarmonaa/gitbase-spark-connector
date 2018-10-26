package tech.sourced.gitbase.spark

import org.apache.spark.sql.SparkSession

package object udf {
  private[udf] var spark: SparkSession = _

  private val udfs = Seq(
    Language,
    Uast,
    UastMode,
    UastXPath,
    UastExtract,
    UastChildren,
    IsBinary
  )

  def isSupported(name: String): Boolean = udfs.exists(f => f.name == name)

  def registerUDFs(ss: SparkSession): Unit = {
    spark = ss
    udfs.foreach(f => spark.udf.register(f.name, f.function.withName(f.name)))
  }
}
