package tech.sourced.gitbase.spark.udf

import org.apache.spark.sql.types.{BinaryType, StructField}
import org.apache.spark.sql.functions._

class UastSpec extends BaseUdfSpec {

  import spark.implicits._

  behavior of "Uast"

  it should "work as a registered UDF" in {
    val uastDf = spark.sqlContext.sql("SELECT *, "
      + Uast.name + "(blob_content,language(file_path, blob_content),'') AS uast FROM " +
      BaseUdfSpec.filesName)

    uastDf.schema.fields should contain(StructField("uast", BinaryType))
  }

  it should "work as an UDF in regular code" in {
    val xpath = ""
    val uastDf = filesDf.withColumn(
      "uast",
      Uast('blob_content, Language('file_path, 'blob_content), lit(xpath))
    )

    uastDf.schema.fields should contain(StructField("uast", BinaryType))
  }

  it should "retrieve UASTs" in {
    val xpath = ""
    val uastDf = filesDf.withColumn(
      "uast",
      Uast('blob_content, Language('file_path, 'blob_content), lit(xpath))
    )

    uastDf.select('file_path, 'uast).collect().foreach(row => row.getString(0) match {
      case ("src/foo.py" | "src/bar.java" | "foo") => {
        val marshaledNodes = row.getAs[Array[Byte]](1)
        marshaledNodes should not be empty

        val nodes = BblfshUtils.unmarshalNodes(marshaledNodes).getOrElse(Seq.empty)
        nodes should not be empty
        nodes should have length 1
      }
      case _ => row.isNullAt(1) should be(true)
    })
  }

  it should "ignore unsupported languages" in {
    val uastDf = filesDf.withColumn(
      "uast",
      Uast('blob_content, lit("text"), lit(""))
    )

    uastDf.select('uast).collect().foreach(row => {
      row.isNullAt(0) should be(true)
    })
  }
}
