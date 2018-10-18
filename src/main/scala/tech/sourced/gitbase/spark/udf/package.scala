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
    UastChildren
  )

  def registerUDFs(ss: SparkSession): Unit = {
    spark = ss
    udfs.foreach(f => spark.udf.register(f.name, f.function))
  }
}
