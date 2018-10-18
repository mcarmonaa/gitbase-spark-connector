package tech.sourced.gitbase.spark.udf

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{BinaryType, StructField}

class UastModeSpec extends BaseUdfSpec {
  import spark.implicits._

  behavior of "UastMode"

  it should "work as a registered UDF" in {
    val uastModeDf = spark.sqlContext.sql("SELECT *,"
      + UastMode.name +"('foo',blob_content,language(file_path,blob_content)) AS uast FROM " +
      BaseUdfSpec.filesName)

    uastModeDf.schema.fields should contain(StructField("uast", BinaryType))
  }

  it should "work as an UDF in regular code" in {
    val uastModeDf = filesDf.withColumn(
      "uast",
      UastMode(lit("annotated"), 'blob_content, Language('file_path, 'blob_content))
    )

    uastModeDf.schema.fields should contain(StructField("uast", BinaryType))
  }

  it should "retrieve UASTs with specific mode" in {
    info("Note that the uast_mode udf checks that the given mode is " +
      "one of (annotated, semantic, native).\nEven though, it will always" +
      " return an annotated uast since org.bblfsh.client.BblfshClient " +
      "doesn't support v2 yet")

    val annotatedDf = filesDf.withColumn(
      "uast",
      UastMode(lit("annotated"), 'blob_content, Language('file_path, 'blob_content))
    )

    annotatedDf.select('file_path, 'uast).collect().foreach(row => row.getString(0) match {
      case ("src/foo.py" | "src/bar.java" | "foo") => {
        val marshaledNodes = row.getAs[Array[Byte]](1)
        marshaledNodes should not be empty

        val nodes = BblfshUtils.unmarshalNodes(marshaledNodes).getOrElse(Seq.empty)
        nodes should not be empty
        nodes should have length 1
      }
      case _ => row.getAs[Array[Byte]](1) should be (null)
    })

    val fooDf = filesDf.withColumn(
      "uast",
      UastMode(lit("foo"), 'blob_content, Language('file_path, 'blob_content))
    )

    fooDf
      .select('uast)
      .collect()
      .foreach(row => row.getAs[Array[Byte]](0) should be (null))
  }
}
