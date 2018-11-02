package tech.sourced.gitbase.spark.udf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{FlatSpec, Matchers, Suite}
import tech.sourced.gitbase.spark.rule
import tech.sourced.gitbase.spark.{defaultConfig, injectRules}

trait BaseUdfSpec extends FlatSpec with Matchers { this: Suite =>

  val spark: SparkSession = SparkSession.builder().appName("test")
    .master("local[*]")
    .config("spark.driver.host", "localhost")
    .config(new SparkConf(false).setAll(defaultConfig))
    .withExtensions(injectRules(rule.getAll))
    .getOrCreate()

  import spark.implicits._
  import BaseUdfSpec._

  registerUDFs(spark)

  val filesDf: DataFrame = filesRows.toDF(filesCols: _*)
  filesDf.createOrReplaceTempView(filesName)

}

private object BaseUdfSpec {
  val filesName = "files"

  val filesRows = Seq(
    ("repo", "path", "bhash", "thash", "entry", null, 0),
    ("repo-1", "src/foo.py", "hash1", "", "",
      "with open('somefile.txt') as f: contents=f.read()".getBytes, 0),
    ("repo-1", "src/bar.java", "hash2", "", "",
      "public class Hello extends GenericServlet { }".getBytes, 0),
    ("repo-1", "src/baz.go", "hash3", "", "", null.asInstanceOf[Array[Byte]], 0),
    ("repo-2", "foo", "hash4", "", "", "#!/usr/bin/env python -tt".getBytes, 0),
    ("repo-2", "bar", "hash5", "", "", null.asInstanceOf[Array[Byte]], 0),
    ("repo-3", "baz", "hash6", "", "", Array[Byte](0,0,0,0), 0)
  )

  val filesCols = Array(
    "repository_id",
    "file_path",
    "blob_hash",
    "tree_hash",
    "tree_entry_mode",
    "blob_content",
    "blob_size"
  )
}
