package tech.sourced.gitbase.spark

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types._

object QueryBuilder {

  private val dialect = GitbaseDialect()

  def qualify(col: Attribute): String = {
    try {
      val table = col.metadata.getString(Sources.SourceKey)
      s"$table.`${col.name}`"
    } catch {
      case _: NoSuchElementException =>
        s"`${col.name}`"
    }
  }

  def qualify(table: String, col: String): String = s"$table.`$col`"

  def compileValue(value: Any): Any = dialect.compileValue(value)

  def compileExpression(e1: Expression, e2: Expression): Option[(String, String)] = {
    (compileExpression(e1), compileExpression(e2)) match {
      case (Some(a), Some(b)) => Some((a, b))
      case _ => None
    }
  }

  private def stripQuotes(str: String): String =
    if (str.startsWith("'")) {
      str.stripPrefix("'").stripSuffix("'")
    } else {
      str.stripPrefix("\"").stripSuffix("\"")
    }

  // scalastyle:off cyclomatic.complexity
  def compileExpression(expr: Expression): Option[String] = {
    expr match {
      case Alias(child, name) => compileExpression(child)
        .map(x => s"$x AS `$name`")
      case ScalaUDF(_, _, children, _, Some(name), _, _) =>
        val args = children.map(compileExpression)
        if (args.forall(_.isDefined) && udf.isSupported(name)) {
          Some(s"`$name`(${args.map(_.get).mkString(", ")})")
        } else {
          None
        }
      case EqualTo(attr, value) =>
        compileExpression(attr, value).map(x => s"${x._1} = ${x._2}")
      case EqualNullSafe(attr: Attribute, value) =>
        compileExpression(attr, value)
          .map(x => s"(NOT (${x._1} != ${x._2} " +
            s"OR ${x._1} IS NULL OR ${x._2} IS NULL) OR " +
            s"(${x._1} IS NULL AND ${x._2} IS NULL))")
      case LessThan(attr, value) =>
        compileExpression(attr, value).map(x => s"${x._1} < ${x._2}")
      case GreaterThan(attr, value) =>
        compileExpression(attr, value).map(x => s"${x._1} > ${x._2}")
      case LessThanOrEqual(attr, value) =>
        compileExpression(attr, value).map(x => s"${x._1} <= ${x._2}")
      case GreaterThanOrEqual(attr, value) =>
        compileExpression(attr, value).map(x => s"${x._1} >= ${x._2}")
      case IsNull(attr) => compileExpression(attr).map(a => s"$a IS NULL")
      case IsNotNull(attr) => compileExpression(attr)
        .map(a => s"$a IS NOT NULL")
      case RLike(left, right) =>
        compileExpression(left, right).map(x => s"${x._1} REGEXP ${x._2}")

      case StartsWith(attr, value) =>
        compileExpression(attr, value)
          .map(x => s"${x._1} REGEXP '^${stripQuotes(x._2)}'")
      case EndsWith(attr, value) =>
        compileExpression(attr, value)
          .map(x => s"${x._1} REGEXP '${stripQuotes(x._2)}$$'")
      case Contains(attr, value) =>
        compileExpression(attr, value)
          .map(x => s"${x._1} REGEXP '${stripQuotes(x._2)}'")

      case In(attr, value) if value.isEmpty =>
        compileExpression(attr)
          .map(a => s"CASE WHEN $a IS NULL THEN NULL ELSE FALSE END")
      case In(attr, value) =>
        (compileExpression(attr), value.map(x => compileExpression(x))) match {
          case (Some(a), b) if b.forall(_.isDefined) =>
            Some(s"$a IN (${b.map(_.get).mkString(", ")})")
          case _ => None
        }
      case Not(f1) => compileExpression(f1).map(p => s"(NOT ($p))")
      case Or(f1, f2) =>
        // We can't compile Or filter unless both sub-filters are compiled successfully.
        // It applies too for the following And filter.
        // If we can make sure compileFilter supports all filters, we can remove this check.
        val or = Seq(f1, f2).flatMap(x => compileExpression(x))
        Option(if (or.size == 2) {
          s"(${or.mkString(") OR (")})"
        } else {
          null
        })
      case And(f1, f2) =>
        val and = Seq(f1, f2).flatMap(x => compileExpression(x))
        Option(if (and.size == 2) {
          s"(${and.mkString(") AND (")})"
        } else {
          null
        })
      case col: Attribute => Some(qualify(col))
      // Literal sql method prints longs as {NUMBER}L, which is not valid SQL. To
      // prevent that, we need to do a special case for longs.
      case Literal(v: Long, _) => Some(v.toString)
      case lit: Literal => Some(lit.sql)
      case Year(e) => compileExpression(e)
        .map(e => s"YEAR($e)")
      case Month(e) => compileExpression(e)
        .map(e => s"MONTH($e)")
      case DayOfMonth(e) => compileExpression(e)
        .map(e => s"DAY($e)")
      case DayOfYear(e) => compileExpression(e)
        .map(e => s"DAYOFYEAR($e)")
      case Round(e, scale) =>
        compileExpression(e, scale).map(x => s"ROUND(${x._1}, ${x._2})")
      case Ceil(e) => compileExpression(e)
        .map(e => s"CEIL($e)")
      case Floor(e) => compileExpression(e)
        .map(e => s"FLOOR($e)")

      case Substring(str, pos, len) =>
        (compileExpression(str), compileExpression(pos), compileExpression(len)) match {
          case (Some(a), Some(b), Some(c)) => Some(s"SUBSTRING($a, $b, $c)")
          case _ => None
        }

      case Cast(e, StringType, _) => compileExpression(e)
        .map(e => s"CAST($e AS CHAR)")
      case Cast(e, BooleanType | IntegerType | ShortType, _) => compileExpression(e)
        .map(e => s"CAST($e AS SIGNED)")
      case Cast(e, TimestampType, _) => compileExpression(e)
        .map(e => s"CAST($e AS DATETIME)")
      case Cast(e, DateType, _) => compileExpression(e)
        .map(e => s"CAST($e AS DATE)")
      case Cast(e, DoubleType | FloatType | LongType, _) => compileExpression(e)
        .map(e => s"CAST($e AS DECIMAL)")
      case Cast(e, _, _) => compileExpression(e)

      case Min(child) => compileExpression(child)
        .map(e => s"MIN($e)")

      case Max(child) => compileExpression(child)
        .map(e => s"MAX($e)")

      case Sum(child) => compileExpression(child)
        .map(e => s"SUM($e)")

      case Count(children) => if (children.length == 1) {
        compileExpression(children.head).map(e => s"COUNT($e)")
      } else {
        None
      }

      case Average(child) => compileExpression(child)
        .map(e => s"AVG($e)")

      // We only compile non-distinct aggregations. Gitbase does not support
      // distinct in aggregations yet.
      case AggregateExpression(fn, _, false, _) =>
        compileExpression(fn)

      case _ => None
    }
  }

  // scalastyle:on cyclomatic.complexity
}

/**
  * Select query builder.
  */
case class QueryBuilder(node: Node = null,
                        schema: StructType = StructType(Seq()),
                        query: Query = Query()) {

  import QueryBuilder._

  def selectedFields(q: Query): String =
    if (q.project.nonEmpty) {
      q.project.flatMap(e => compileExpression(e)).mkString(", ")
    } else {
      // when there is no field selected, such as a count of repositories,
      // just get the first field to avoid returning all the fields
      schema.fields.headOption match {
        case Some(f) => qualify(AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
        case None =>
          throw new SparkException("unable to build sql query with no columns")
      }
    }

  def whereClause(q: Query = Query()): String = {
    val compiledFilters = q.filters.flatMap(x => compileExpression(x))
    if (compiledFilters.isEmpty) {
      ""
    } else {
      s" WHERE (${compiledFilters.mkString(") AND (")})"
    }
  }

  def orderByClause(query: Query = Query()): String = if (query.sort.isEmpty) {
    ""
  } else {
    s" ORDER BY ${query.sort.flatMap(orderByField).mkString(", ")}"
  }

  def limitClause(query: Query = Query()): String =
    query.limit.map(x => s" LIMIT ${x.toString.stripSuffix("L")}").getOrElse("")

  def orderByField(field: SortOrder): Option[String] = {
    compileExpression(field.child)
      .map(x => s"$x ${field.direction.sql}")
  }

  def groupByClause(query: Query = Query()): String = if (query.grouping.isEmpty) {
    ""
  } else {
    s" GROUP BY ${query.grouping.flatMap(x => compileExpression(x)).mkString(", ")}"
  }

  def getOnClause(cond: Expression): String = compileExpression(cond)
    .getOrElse(throw new SparkException(s"unable to compile expression $cond"))

  def sourceToSql(source: Node): String = source match {
    case Join(left, right, Some(cond)) =>
      s"${sourceToSql(left)} INNER JOIN ${sourceToSql(right)} ON ${getOnClause(cond)}"
    case Join(left, right, None) =>
      s"${sourceToSql(left)} JOIN ${sourceToSql(right)}"
    case Table(name) => name
    case _ => throw new SparkException("invalid node found in query source")
  }

  def subqueryToSql(q: Query): String = s"(${queryToSql(q)}) AS `t`"

  def selectedTables(q: Query = Query()): String = (q.subquery, q.source) match {
    case (Some(_), Some(_)) =>
      throw new SparkException("This is likely a bug. Found source and subquery in the query.")
    case (Some(subquery), None) =>
      subqueryToSql(subquery)
    case (None, Some(source)) =>
      sourceToSql(source)
    case (None, None) =>
      throw new SparkException("no source or subquery found in query")
  }

  def queryToSql(q: Query): String =
    s"SELECT ${selectedFields(q)} FROM ${selectedTables(q)}${whereClause(q)}" +
      s"${groupByClause(q)}${orderByClause(q)}${limitClause(q)}"

  /**
    * Returns the built select SQL query.
    *
    * @return select SQL query
    */
  def sql: String = {
    queryToSql(node.buildQuery(query))
  }

}
