package tech.sourced.gitbase.spark.udf

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{BinaryType, StructField}

class UastXPathSpec extends BaseUdfSpec {
  import spark.implicits._

  behavior of "UastXPath"

  it should "work as a registered UDF" in {
    val filteredDf = spark.sqlContext.sql("SELECT *, " + UastXPath.name +
      "(uast(blob_content, language(file_path,blob_content), '')," +
      " '//*[@roleFunction]') AS filtered FROM " + BaseUdfSpec.filesName)

    filteredDf.schema.fields should contain(StructField("filtered", BinaryType))
  }

  it should "work as an UDF in regular code" in {
    val xpath = "//*[@roleFunction]"
    val filteredDf = filesDf.withColumn(
      "filtered",
      UastXPath(
        UastMode(lit("annotated"), 'blob_content, Language('file_path, 'blob_content)),
        lit(xpath)
      )
    )

    filteredDf.schema.fields should contain(StructField("filtered", BinaryType))
  }

  it should "filter UASTs with the given XPath query" in {
    val xpath = "//*[@roleFunction]"
    val filteredDf = filesDf.withColumn(
      "filtered",
      UastXPath(
        UastMode(lit("annotated"), 'blob_content, Language('file_path, 'blob_content)),
        lit(xpath)
      )
    )

    filteredDf.select('file_path, 'filtered).collect().foreach(row => row.getString(0) match {
      case "src/foo.py" => row.getAs[Array[Byte]](1) should not be empty
      case _ => row.getAs[Array[Byte]](1) should be (null)
    })
  }
}
