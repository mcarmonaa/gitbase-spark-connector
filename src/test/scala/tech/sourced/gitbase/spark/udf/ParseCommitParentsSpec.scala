package tech.sourced.gitbase.spark.udf

import org.apache.spark.sql.types.{ArrayType, StringType, StructField}

class ParseCommitParentsSpec extends BaseUdfSpec {

  import spark.implicits._

  behavior of "ParseCommitParents"

  private val parentsDf = Seq(
    ("[\"foo\", \"bar\"]".getBytes, ""),
    ("[\"foo\", \"bar\", \"baz\"]".getBytes, "")
  ).toDF("commit_parents", "foo")

  parentsDf.createOrReplaceTempView("commits_parents")

  it should "work as a registered UDF" in {
    val df = spark.sql("SELECT parse_commit_parents(commit_parents) as " +
      "parents FROM commits_parents")

    df.schema.fields should contain(StructField("parents", ArrayType(StringType)))
  }

  it should "work as an UDF in regular code" in {
    val df = spark.table("commits_parents")
      .withColumn("parents", ParseCommitParents('commit_parents))

    df.schema.fields should contain(StructField("parents", ArrayType(StringType)))
  }

  it should "extract parents from commits" in {
    val rows = spark.table("commits_parents")
      .withColumn("parents", ParseCommitParents('commit_parents))
      .select("parents")
      .collect()
      .map(row => row(0))

    rows should equal(Array(
      Array("foo", "bar"),
      Array("foo", "bar", "baz")
    ))
  }
}
