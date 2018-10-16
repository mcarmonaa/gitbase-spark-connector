package tech.sourced.gitbase.spark.udf

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{BinaryType, StructField}

class UastChildrenSpec extends BaseUdfSpec {
  import spark.implicits._

  behavior of "UastChildren"

  it should "work as a registered UDF" in {
    val childrenDf = spark.sqlContext.sql("SELECT *, " + UastChildren.name +
      "(uast(blob_content, language(file_path,blob_content), '')) AS children FROM " +
      BaseUdfSpec.filesName)

    childrenDf.schema.fields should contain(StructField("children", BinaryType))
  }

  it should "work as an UDF in regular code" in {
    val childrenDf = filesDf.withColumn(
      "children",
      UastChildren(UastMode(
        lit("annotated"),
        'blob_content, Language('file_path, 'blob_content)
      ))
    )

    childrenDf.schema.fields should contain(StructField("children", BinaryType))
  }

  it should "return the children of the given nodes" in {
    val childrenDf = filesDf.withColumn(
      "children",
      UastChildren(UastMode(
        lit("annotated"),
        'blob_content, Language('file_path, 'blob_content)
      ))
    )

    childrenDf.select('file_path, 'children).collect().foreach(row => row.getString(0) match {
      case ("src/foo.py" | "src/bar.java" | "foo") => row.getAs[Array[Byte]](1) should not be empty
      case _ => row.getAs[Array[Byte]](1) should be (null)
    })
  }
}
