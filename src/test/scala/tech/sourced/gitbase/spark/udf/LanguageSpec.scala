package tech.sourced.gitbase.spark.udf

import org.apache.spark.sql.types.{StringType, StructField}

class LanguageSpec extends BaseUdfSpec {
  import spark.implicits._
  import LanguageSpec._

  behavior of "Language UDF"

  it should "guess files' language properly" in {
    test.foreach(args =>
      Language.get(args.path, args.content) match {
        case Some(lang) => lang shouldBe args.expected
        case None => args.expected shouldBe ""
      })
  }

  it should "work correctly" in {
    val languagesDf = files.toDF(filesCols: _*)
      .withColumn("lang", Language('path, 'content))

    languagesDf.schema.fields should contain(StructField("lang", StringType))
  }

  it should "guess the correct language" in {
    val languagesDf = files.toDF(filesCols: _*)
      .withColumn("lang", Language('path, 'content))

    languagesDf.select('path, 'lang).collect().foreach(row => row.getString(0) match {
      case "foo.py" => row.getString(1) should be("Python")
      case "bar.java" => row.getString(1) should be("Java")
      case "baz.go" => row.getString(1) should be("Go")
      case "no-filename" => row.getString(1) should be("Python")
      case _ => row.getString(1) should be(null)
    })
  }

  it should "work as a registered udf" in {
    spark.udf.register(Language.name, Language.function)
    files.toDF(filesCols: _*).createOrReplaceTempView("files")

    val languagesDf = spark.sqlContext.sql("SELECT *, "
      + Language.name + "(path, content) AS lang FROM files")
    languagesDf.schema.fields should contain(StructField("lang", StringType))
  }
}

object LanguageSpec {
  case class LangArgs(expected: String, path: String, content: Array[Byte])

  val test = Seq(
    LangArgs("Python", "foo.py", null),
    LangArgs("Go", "main.go", null),
    LangArgs("", "a",
      """
        |package main
        |
        |import fmt
        |
        |func main() {
        |        fmt.Println("Hello World!")
        |}
        |-
      """.stripMargin.getBytes()),
    LangArgs("Shell", "b", "#!/bin/sh".getBytes())
  )

  val files = Seq(
    ("foo.py", "with open('somefile.txt') as f: contents=f.read()".getBytes),
    ("bar.java", "public class Hello extends GenericServlet { }".getBytes),
    ("baz.go", null.asInstanceOf[Array[Byte]]),
    ("no-filename", "#!/usr/bin/env python -tt".getBytes()),
    ("unknown", null.asInstanceOf[Array[Byte]]),
    ("binary-file", Array[Byte](0, 0, 0, 0))
  )

  val filesCols = Array("path", "content")
}
