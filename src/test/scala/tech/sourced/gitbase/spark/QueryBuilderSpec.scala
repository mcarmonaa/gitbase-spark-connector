package tech.sourced.gitbase.spark

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.{FlatSpec, Matchers}
import QueryBuilder._
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.Column
import tech.sourced.gitbase.spark.udf._

class QueryBuilderSpec extends FlatSpec with Matchers {

  import QueryBuilderUtil._

  "QueryBuilder.qualify" should "qualify and quote col" in {
    val expected = s"foo.`bar`"
    qualify("foo", "bar") should be(expected)
  }

  "QueryBuilder.compileValue" should "return compiled value" in {
    val now = System.currentTimeMillis
    val cases = Seq(
      (UTF8String.fromString("foo"), "'foo'"),
      ("fo'o", "'fo''o'"),
      (new Timestamp(now), s"'${new Timestamp(now)}'"),
      (new Date(now), s"'${new Date(now)}'"),
      (Seq("a", 1, true), "'a', 1, 1"),
      (true, 1),
      (false, 0)
    )

    cases.foreach {
      case (input, expected) =>
        val output = compileValue(input)
        output should be(expected)
    }
  }

  "QueryBuilder.sql" should "return the SQL for a tree" in {
    QueryBuilder(
      Sort(
        Seq(
          SortOrder(mkAttr("foo", "a"), Ascending)
        ),
        Project(
          Seq(
            mkAttr("foo", "a"),
            mkAttr("foo", "b"),
            mkAttr("bar", "c")
          ),
          Filter(
            Seq(EqualTo(
              mkAttr("foo", "a"),
              Literal(2, IntegerType)
            )),
            Join(
              Table("foo"),
              Table("bar"),
              Some(EqualTo(
                mkAttr("foo", "a"),
                mkAttr("bar", "a")
              ))
            )
          )
        )
      )
    ).sql should be("SELECT foo.`a`, foo.`b`, bar.`c` " +
      "FROM foo INNER JOIN bar ON foo.`a` = bar.`a` " +
      "WHERE (foo.`a` = 2) " +
      "ORDER BY foo.`a` ASC")
  }

  "QueryBuilder.whereClause" should "return SQL for where clause" in {
    val qb = QueryBuilder()
    qb.whereClause() should be("")

    qb.whereClause(Query(filters = Seq(
      EqualTo(
        mkAttr("foo", "bar"),
        Literal(1, IntegerType)
      )
    ))) should be(s"WHERE (${qualify("foo", "bar")} = 1)")

    qb.whereClause(Query(filters = Seq(
      EqualTo(
        mkAttr("foo", "bar"),
        Literal(1, IntegerType)
      ),
      EqualTo(
        mkAttr("foo", "baz"),
        Literal(2, IntegerType)
      )
    ))) should be(s"WHERE (${qualify("foo", "bar")} = 1) " +
      s"AND (${qualify("foo", "baz")} = 2)")
  }

  "QueryBuilder.selectedTables" should "return SQL for FROM clause" in {
    val qb = QueryBuilder()
    qb.selectedTables(Query(
      source = Some(
        Join(
          Table("foo"),
          Table("bar"),
          None
        ))
    )) should be("foo JOIN bar")

    qb.selectedTables(Query(source = Some(Join(
      Table("foo"),
      Table("bar"),
      Some(EqualTo(
        mkAttr("foo", "a"),
        mkAttr("bar", "a")
      ))
    )))) should be("foo INNER JOIN bar ON foo.`a` = bar.`a`")

    qb.selectedTables(Query(source = Some(Join(
      Join(
        Table("foo"),
        Table("bar"),
        Some(EqualTo(
          mkAttr("foo", "a"),
          mkAttr("bar", "a")
        ))
      ),
      Table("baz"),
      Some(EqualTo(
        mkAttr("foo", "a"),
        mkAttr("baz", "a")
      ))
    )))) should be("foo INNER JOIN bar ON foo.`a` = bar.`a` " +
      "INNER JOIN baz ON foo.`a` = baz.`a`")
  }

  "QueryBuilder.orderByClause" should "return SQL for ORDER BY clause" in {
    val qb = QueryBuilder()

    qb.orderByClause() should be("")

    qb.orderByClause(Query(sort = Seq(
      SortOrder(
        mkAttr("foo", "a"),
        Ascending
      )
    ))) should be(" ORDER BY foo.`a` ASC")

    qb.orderByClause(Query(sort = Seq(
      SortOrder(
        mkAttr("foo", "a"),
        Ascending
      ),
      SortOrder(
        mkAttr("foo", "b"),
        Descending
      )
    ))) should be(" ORDER BY foo.`a` ASC, foo.`b` DESC")
  }

  "QueryBuilder.compileExpression" should "compile the expressions to SQL" in {
    val cases = Seq(
      (EqualTo(mkAttr("foo", "a"), Literal(1, IntegerType)),
        "foo.`a` = 1"),

      (EqualNullSafe(mkAttr("foo", "a"), Literal(1, IntegerType)),
        s"(NOT (foo.`a` != 1 OR foo.`a` IS NULL OR 1 IS NULL) " +
          s"OR (foo.`a` IS NULL AND 1 IS NULL))"),

      (LessThan(mkAttr("foo", "a"), Literal(1, IntegerType)),
        s"foo.`a` < 1"),

      (GreaterThan(mkAttr("foo", "a"), Literal(1, IntegerType)),
        s"foo.`a` > 1"),

      (LessThanOrEqual(mkAttr("foo", "a"), Literal(1, IntegerType)),
        s"foo.`a` <= 1"),

      (GreaterThanOrEqual(mkAttr("foo", "a"), Literal(1, IntegerType)),
        s"foo.`a` >= 1"),

      (IsNull(mkAttr("foo", "a")),
        "foo.`a` IS NULL"),

      (IsNotNull(mkAttr("foo", "a")),
        "foo.`a` IS NOT NULL"),

      (In(mkAttr("foo", "a"), Seq[Expression]()),
        "CASE WHEN foo.`a` IS NULL THEN NULL ELSE FALSE END"),

      (In(mkAttr("foo", "a"), Seq(Literal(1, IntegerType), Literal(2, IntegerType))),
        "foo.`a` IN (1, 2)"),

      (Not(EqualTo(mkAttr("foo", "a"), Literal(1, IntegerType))),
        "(NOT (foo.`a` = 1))"),

      (Or(EqualTo(mkAttr("foo", "a"), Literal(1, IntegerType)),
        EqualTo(mkAttr("foo", "b"), Literal(2, IntegerType))),
        "(foo.`a` = 1) OR (foo.`b` = 2)"),

      (RLike(mkAttr("foo", "a"), Literal("'foo'", StringType)),
        "foo.`a` REGEXP 'foo'"),

      (Year(mkAttr("foo", "a")), "YEAR(foo.`a`)"),

      (Month(mkAttr("foo", "a")), "MONTH(foo.`a`)"),

      (DayOfMonth(mkAttr("foo", "a")), "DAY(foo.`a`)"),

      (DayOfYear(mkAttr("foo", "a")), "DAYOFYEAR(foo.`a`)"),

      (Round(mkAttr("foo", "a"), Literal(1, IntegerType)), "ROUND(foo.`a`, 1)"),

      (Ceil(mkAttr("foo", "a")), "CEIL(foo.`a`)"),

      (Floor(mkAttr("foo", "a")), "FLOOR(foo.`a`)"),

      (Substring(
        mkAttr("foo", "a"),
        Literal(1, IntegerType),
        Literal(2, IntegerType)
      ), "SUBSTRING(foo.`a`, 1, 2)"),

      (Cast(mkAttr("foo", "a"), IntegerType, None), "CAST(foo.`a` AS INT)"),

      (And(
        expressions.RLike(
          mkAttr("foo", "a"),
          Literal("'^refs/heads/HEAD/'", StringType)
        ),
        Or(
          expressions.RLike(
            mkAttr("foo", "a"),
            Literal("""'(?i)facebook.*[\'\\"][0-9a-f]{32}[\'\\"]'""", StringType)
          ),
          Or(
            expressions.RLike(
              mkAttr("foo", "a"),
              Literal("""'(?i)twitter.*[\'\\"][0-9a-zA-Z]{35,44}[\'\\"]'""", StringType)
            ),
            Or(
              expressions.RLike(
                mkAttr("foo", "a"),
                Literal("""'(?i)github.*[\'\\"][0-9a-zA-Z]{35,40}[\'\\"]'""", StringType)
              ),
              Or(
                expressions.RLike(
                  mkAttr("foo", "a"),
                  Literal("""'AKIA[0-9A-Z]{16}'""", StringType)
                ),
                Or(
                  expressions.RLike(
                    mkAttr("foo", "a"),
                    Literal("""'(?i)reddit.*[\'\\"][0-9a-zA-Z]{14}[\'\\"]'""", StringType)
                  ),
                  Or(
                    expressions.RLike(
                      mkAttr("foo", "a"),
                      Literal(
                        """'(?i)heroku.*[0-9A-F]{8}-[0-9A-F]{4}-"""
                          +
                          """[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}'""", StringType)
                    ),
                    Or(
                      expressions.RLike(
                        mkAttr("foo", "a"),
                        Literal("""'.*-----BEGIN PRIVATE KEY-----.*'""", StringType)
                      ),
                      Or(
                        expressions.RLike(
                          mkAttr("foo", "a"),
                          Literal("""'.*-----BEGIN RSA PRIVATE KEY-----.*'""", StringType)
                        ),
                        Or(
                          expressions.RLike(
                            mkAttr("foo", "a"),
                            Literal("""'.*-----BEGIN DSA PRIVATE KEY-----.*'""", StringType)
                          ),
                          expressions.RLike(
                            mkAttr("foo", "a"),
                            Literal("""'.*-----BEGIN OPENSSH PRIVATE KEY-----.*'""", StringType)
                          )
                        )
                      )
                    )
                  )
                )
              )
            )
          )
        )
      ),
        "(foo.`a` REGEXP '^refs/heads/HEAD/') AND " +
          """((foo.`a` REGEXP '(?i)facebook.*[\'\\"][0-9a-f]{32}[\'\\"]') OR """ +
          """((foo.`a` REGEXP '(?i)twitter.*[\'\\"][0-9a-zA-Z]{35,44}[\'\\"]') OR """ +
          """((foo.`a` REGEXP '(?i)github.*[\'\\"][0-9a-zA-Z]{35,40}[\'\\"]') OR """ +
          """((foo.`a` REGEXP 'AKIA[0-9A-Z]{16}') OR """ +
          """((foo.`a` REGEXP '(?i)reddit.*[\'\\"][0-9a-zA-Z]{14}[\'\\"]') OR """ +
          """((foo.`a` REGEXP '(?i)heroku.*[0-9A-F]""" +
          """{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}') OR """ +
          """((foo.`a` REGEXP '.*-----BEGIN PRIVATE KEY-----.*') OR """ +
          """((foo.`a` REGEXP '.*-----BEGIN RSA PRIVATE KEY-----.*') OR """ +
          """((foo.`a` REGEXP '.*-----BEGIN DSA PRIVATE KEY-----.*') OR """ +
          """(foo.`a` REGEXP '.*-----BEGIN OPENSSH PRIVATE KEY-----.*'))))))))))""")
    )

    cases.foreach {
      case (expr, expected) =>
        compileExpression(expr).get should be(expected)
    }
  }

  "QueryBuilder.compileExpression" should "compile udfs" in {
    val cases = Seq(
      (Language(mkCol("foo", "a"), mkCol("foo", "b")),
        "`language`(foo.`a`, foo.`b`)"),

      (Uast(mkCol("foo", "a"), mkCol("foo", "b"), mkCol("foo", "c")),
        "`uast`(foo.`a`, foo.`b`, foo.`c`)"),

      (UastChildren(mkCol("foo", "a")),
        "`uast_children`(foo.`a`)"),

      (UastExtract(mkCol("foo", "a"), mkCol("foo", "b")),
        "`uast_extract`(foo.`a`, foo.`b`)"),

      (UastMode(mkCol("foo", "a"), mkCol("foo", "b"), mkCol("foo", "c")),
        "`uast_mode`(foo.`a`, foo.`b`, foo.`c`)"),

      (UastXPath(mkCol("foo", "a"), mkCol("foo", "b")),
        "`uast_xpath`(foo.`a`, foo.`b`)")
    )

    cases.foreach {
      case (udf, expected) =>
        compileExpression(udf.expr).get should be(expected)
    }
  }
}

object QueryBuilderUtil {
  def mkAttr(table: String, name: String): Attribute = {
    val metadata = new MetadataBuilder().putString(Sources.SourceKey, table).build()
    AttributeReference(name, StringType, nullable = false, metadata)()
  }

  def mkCol(table: String, name: String): Column = {
    new Column(mkAttr(table, name))
  }
}
