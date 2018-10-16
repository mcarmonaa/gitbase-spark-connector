package tech.sourced.gitbase.spark

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.{FlatSpec, Matchers}
import QueryBuilder._
import org.apache.spark.sql.catalyst.expressions

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

  "QueryBuilder.compileFilter" should "compile the filters to SQL" in {
    val col = qualify("foo", "bar")
    val cases = Seq(
      (EqualTo(col, Literal(1, IntegerType)), s"$col = 1"),
      (EqualNullSafe(col, Literal(1, IntegerType)),
        s"(NOT ($col != 1 OR $col IS NULL OR 1 IS NULL) OR ($col IS NULL AND 1 IS NULL))"),
      (LessThan(col, Literal(1, IntegerType)), s"$col < 1"),
      (GreaterThan(col, Literal(1, IntegerType)), s"$col > 1"),
      (LessThanOrEqual(col, Literal(1, IntegerType)), s"$col <= 1"),
      (GreaterThanOrEqual(col, Literal(1, IntegerType)), s"$col >= 1"),
      (IsNull(col), s"$col IS NULL"),
      (IsNotNull(col), s"$col IS NOT NULL"),
      (In(col, Array()), s"CASE WHEN $col IS NULL THEN NULL ELSE FALSE END"),
      (In(col, Array(Literal(1, IntegerType), Literal(2, IntegerType))), s"$col IN (1, 2)"),
      (Not(EqualTo(col, Literal(1, IntegerType))), s"(NOT ($col = 1))"),
      (Or(EqualTo(col, Literal(1, IntegerType)),
        EqualTo(col, Literal(2, IntegerType))), s"($col = 1) OR ($col = 2)"),
      (And(EqualTo(col, Literal(1, IntegerType)),
        EqualTo(col, Literal(2, IntegerType))), s"($col = 1) AND ($col = 2)")
    )

    cases.foreach {
      case (expr, expected) =>
        compileFilter(expr).get should be(expected)
    }
  }

  "QueryBuilder.whereClause" should "return SQL for where clause" in {
    QueryBuilder().whereClause should be("")

    QueryBuilder(filters = Seq(
      EqualTo(qualify("foo", "bar"), Literal(1, IntegerType))
    )).whereClause should be(s"WHERE ${qualify("foo", "bar")} = 1")

    QueryBuilder(filters = Seq(
      EqualTo(qualify("foo", "bar"), Literal(1, IntegerType)),
      EqualTo(qualify("foo", "baz"), Literal(2, IntegerType))
    )).whereClause should be(s"WHERE ${qualify("foo", "bar")} = 1 AND ${qualify("foo", "baz")} = 2")
  }

  "QueryBuilder.selectedTables" should "return SQL for FROM clause" in {
    QueryBuilder(
      source = JoinedSource(TableSource("foo"), TableSource("bar"), None)
    ).selectedTables should be("foo JOIN bar")

    QueryBuilder(source = JoinedSource(
      TableSource("foo"),
      TableSource("bar"),
      Some(expressions.EqualTo(
        mkAttr("foo", "a"),
        mkAttr("bar", "a")
      ))
    )).selectedTables should be("foo INNER JOIN bar ON foo.a = bar.a")

    QueryBuilder(source = JoinedSource(
      JoinedSource(
        TableSource("foo"),
        TableSource("bar"),
        Some(expressions.EqualTo(
          mkAttr("foo", "a"),
          mkAttr("bar", "a")
        ))
      ),
      TableSource("baz"),
      Some(expressions.EqualTo(
        mkAttr("foo", "a"),
        mkAttr("baz", "a")
      ))
    )).selectedTables should be("foo INNER JOIN bar ON foo.a = bar.a " +
      "INNER JOIN baz ON foo.a = baz.a")
  }

  "QueryBuilder.compileExpression" should "compile the expressions to SQL" in {
    import org.apache.spark.sql.catalyst.expressions._

    val cases = Seq(
      (EqualTo(mkAttr("foo", "a"), Literal(1, IntegerType)),
        "foo.a = 1"),

      (EqualNullSafe(mkAttr("foo", "a"), Literal(1, IntegerType)),
        s"(NOT (foo.a != 1 OR foo.a IS NULL OR 1 IS NULL) OR (foo.a IS NULL AND 1 IS NULL))"),

      (LessThan(mkAttr("foo", "a"), Literal(1, IntegerType)),
        s"foo.a < 1"),

      (GreaterThan(mkAttr("foo", "a"), Literal(1, IntegerType)),
        s"foo.a > 1"),

      (LessThanOrEqual(mkAttr("foo", "a"), Literal(1, IntegerType)),
        s"foo.a <= 1"),

      (GreaterThanOrEqual(mkAttr("foo", "a"), Literal(1, IntegerType)),
        s"foo.a >= 1"),

      (IsNull(mkAttr("foo", "a")),
        "foo.a IS NULL"),

      (IsNotNull(mkAttr("foo", "a")),
        "foo.a IS NOT NULL"),

      (In(mkAttr("foo", "a"), Seq[Expression]()),
        "CASE WHEN foo.a IS NULL THEN NULL ELSE FALSE END"),

      (In(mkAttr("foo", "a"), Seq(Literal(1, IntegerType), Literal(2, IntegerType))),
        "foo.a IN (1, 2)"),

      (Not(EqualTo(mkAttr("foo", "a"), Literal(1, IntegerType))),
        "(NOT (foo.a = 1))"),

      (Or(EqualTo(mkAttr("foo", "a"), Literal(1, IntegerType)),
        EqualTo(mkAttr("foo", "b"), Literal(2, IntegerType))),
        "(foo.a = 1) OR (foo.b = 2)"),

      (And(
        EqualTo(mkAttr("foo", "a"), Literal(1, IntegerType)),
        EqualTo(mkAttr("foo", "b"), Literal(2, IntegerType))
      ), "(foo.a = 1) AND (foo.b = 2)")
    )

    cases.foreach {
      case (expr, expected) =>
        compileExpression(expr).get should be(expected)
    }
  }
}

object QueryBuilderUtil {
  def mkAttr(table: String, name: String): expressions.Attribute = {
    val metadata = new MetadataBuilder().putString(Sources.SourceKey, table).build()
    expressions.AttributeReference(name, StringType, nullable = false, metadata)()
  }
}
