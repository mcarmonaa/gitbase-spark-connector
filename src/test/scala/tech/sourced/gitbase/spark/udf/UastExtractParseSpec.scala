package tech.sourced.gitbase.spark.udf

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField}

class UastExtractParseSpec extends BaseUdfSpec {

  import spark.implicits._

  behavior of "UastExtractParse"

  it should "work as a registered UDF" in {
    val rolesDf = spark.sqlContext.sql("SELECT *, " +
      "uast_extract_parse(uast_extract(uast(blob_content, language(file_path,blob_content), '')," +
      " '@role')) AS roles FROM " + BaseUdfSpec.filesName)

    rolesDf.schema.fields should contain(StructField("roles", ArrayType(StringType)))
  }

  it should "work as an UDF in regular code" in {
    val rolesDf = filesDf.withColumn(
      "roles",
      UastExtractParse(
        UastExtract(
          UastMode(lit("annotated"), 'blob_content, Language('file_path, 'blob_content)),
          lit("@role")
        )))

    rolesDf.schema.fields should contain(StructField("roles", ArrayType(StringType)))
  }

  it should "extract properties from UAST nodes" in {
    val keys = Seq(
      "@role",
      "@type",
      "@token",
      "@startpos",
      "@endpos",
      "foo"
    )

    keys.foreach(key => {
      val extractDf = filesDf.withColumn(
        key,
        UastExtractParse(
          UastExtract(
            UastMode(lit("annotated"), 'blob_content, Language('file_path, 'blob_content)),
            lit(key)
          )))

      extractDf.select('file_path, col(key)).collect().foreach(row => row.getString(0) match {
        case "src/foo.py" | "src/bar.java" | "foo" if Seq("@token", "foo").contains(key) =>
          row.getAs[Seq[String]](1) should be(empty)
        case "src/foo.py" | "src/bar.java" | "foo" => row.getAs[Seq[String]](1) should not be empty
        case _ => row.getAs[Seq[String]](1) should be(null)
      })

    })
  }
}
