package tech.sourced.gitbase.spark

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.StructType

object QueryBuilder {

  private val dialect = GitbaseDialect()

  def qualify(col: Attribute): String = {
    val table = col.metadata.getString(Sources.SourceKey)
    s"$table.`${col.name}`"
  }

  def qualify(table: String, col: String): String = s"$table.`$col`"

  def compileValue(value: Any): Any = dialect.compileValue(value)

  def compileExpression(e1: Expression, e2: Expression): Option[(String, String)] = {
    (compileExpression(e1), compileExpression(e2)) match {
      case (Some(a), Some(b)) => Some((a, b))
      case _ => None
    }
  }

  // scalastyle:off cyclomatic.complexity
  def compileExpression(expr: Expression): Option[String] = {
    expr match {
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

      /* TODO(erizocosmico): support this
    case expressions.StringStartsWith(attr, value) => s"""$attr REGEXP '^$value'"""
    case expressions.StringEndsWith(attr, value) => s"""$attr REGEXP '$value$$'"""
    case expressions.StringContains(attr, value) => s"""$attr REGEXP '$value'"""
    */

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
          or.map(p => s"($p)").mkString(" OR ")
        } else {
          null
        })
      case And(f1, f2) =>
        val and = Seq(f1, f2).flatMap(x => compileExpression(x))
        Option(if (and.size == 2) {
          and.map(p => s"($p)").mkString(" AND ")
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

      case Cast(e, typ, _) => compileExpression(e)
        .map(e => s"CAST($e AS ${typ.sql})")
      case _ => None
    }
  }

  // scalastyle:on cyclomatic.complexity
}

/**
  * Select query builder.
  *
  * @param fields  fields to select
  * @param source  source to get data from
  * @param filters filters to apply
  */
case class QueryBuilder(fields: Seq[Attribute] = Seq(),
                        source: DataSource = null,
                        filters: Seq[Expression] = Seq(),
                        schema: StructType = StructType(Seq())) {

  import QueryBuilder._

  def selectedFields: String =
    if (fields.nonEmpty) {
      fields.map(qualify).mkString(", ")
    } else {
      // when there is no field selected, such as a count of repositories,
      // just get the first field to avoid returning all the fields
      schema.fields.headOption match {
        case Some(f) => qualify(AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
        case None =>
          throw new SparkException("unable to build sql query with no columns")
      }
    }

  def whereClause: String = {
    val compiledFilters = filters.flatMap(x => compileExpression(x))
    if (compiledFilters.isEmpty) {
      ""
    } else {
      s"WHERE ${compiledFilters.mkString(" AND ")}"
    }
  }

  def getOnClause(cond: Expression): String = compileExpression(cond)
    .getOrElse(throw new SparkException(s"unable to compile expression $cond"))

  def sourceToSql(source: DataSource): String = source match {
    case JoinedSource(left, right, Some(cond)) =>
      s"${sourceToSql(left)} INNER JOIN ${sourceToSql(right)} ON ${getOnClause(cond)}"
    case JoinedSource(left, right, None) =>
      s"${sourceToSql(left)} JOIN ${sourceToSql(right)}"
    case TableSource(name) => name
  }

  def selectedTables: String = sourceToSql(source)

  /**
    * Returns the built select SQL query.
    *
    * @return select SQL query
    */
  def sql: String =
    s"SELECT $selectedFields FROM $selectedTables $whereClause"

}
