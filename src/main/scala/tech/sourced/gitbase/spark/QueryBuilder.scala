package tech.sourced.gitbase.spark

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.StructType

object QueryBuilder {

  private val dialect = GitbaseDialect()

  def qualify(col: Attribute): String = {
    val table = col.metadata.getString(Sources.SourceKey)
    s"$table.${col.name}"
  }

  def qualify(table: String, col: String): String = s"$table.`$col`"

  def compileValue(value: Any): Any = dialect.compileValue(value)

  def mustCompileExpr(expr: Expression): String =
    compileExpression(expr)
      .getOrElse(throw new SparkException(s"unable to compile expression: $expr"))

  def compileExpression(expr: Expression): Option[String] = {
    Option(expr match {
      case EqualTo(attr, value) =>
        s"${mustCompileExpr(attr)} = ${mustCompileExpr(value)}"
      case EqualNullSafe(attr: Attribute, value) =>
        val col = attr
        s"(NOT (${mustCompileExpr(col)} != ${mustCompileExpr(value)} " +
          s"OR ${mustCompileExpr(col)} IS NULL OR " +
          s"${mustCompileExpr(value)} IS NULL) OR " +
          s"(${mustCompileExpr(col)} IS NULL " +
          s"AND ${mustCompileExpr(value)} IS NULL))"
      case LessThan(attr, value) =>
        s"${mustCompileExpr(attr)} < ${mustCompileExpr(value)}"
      case GreaterThan(attr, value) =>
        s"${mustCompileExpr(attr)} > ${mustCompileExpr(value)}"
      case LessThanOrEqual(attr, value) =>
        s"${mustCompileExpr(attr)} <= ${mustCompileExpr(value)}"
      case GreaterThanOrEqual(attr, value) =>
        s"${mustCompileExpr(attr)} >= ${mustCompileExpr(value)}"
      case IsNull(attr) => s"${mustCompileExpr(attr)} IS NULL"
      case IsNotNull(attr) => s"""${mustCompileExpr(attr)} IS NOT NULL"""

      /* TODO(erizocosmico): support this
    case expressions.StringStartsWith(attr, value) => s"""$attr REGEXP '^$value'"""
    case expressions.StringEndsWith(attr, value) => s"""$attr REGEXP '$value$$'"""
    case expressions.StringContains(attr, value) => s"""$attr REGEXP '$value'"""
    */

      case In(attr, value) if value.isEmpty =>
        s"""CASE WHEN ${mustCompileExpr(attr)} IS NULL THEN NULL ELSE FALSE END"""
      case In(attr, value) =>
        s"""${mustCompileExpr(attr)} IN (${value.map(mustCompileExpr).mkString(", ")})"""
      case Not(f1) => compileExpression(f1).map(p => s"(NOT ($p))").orNull
      case Or(f1, f2) =>
        // We can't compile Or filter unless both sub-filters are compiled successfully.
        // It applies too for the following And filter.
        // If we can make sure compileFilter supports all filters, we can remove this check.
        val or = Seq(f1, f2).flatMap(compileExpression)
        if (or.size == 2) {
          or.map(p => s"($p)").mkString(" OR ")
        } else {
          null
        }
      case And(f1, f2) =>
        val and = Seq(f1, f2).flatMap(compileExpression)
        if (and.size == 2) {
          and.map(p => s"($p)").mkString(" AND ")
        } else {
          null
        }
      case col: Attribute => qualify(col)
      // Literal sql method prints longs as {NUMBER}L, which is not valid SQL. To
      // prevent that, we need to do a special case for longs.
      case Literal(v: Long, _) => v.toString
      case lit: Literal => lit.sql
      case _ => null
    })
  }
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
    val compiledFilters = filters.flatMap(compileExpression)
    if (compiledFilters.isEmpty) {
      ""
    } else {
      s"WHERE ${compiledFilters.mkString(" AND ")}"
    }
  }

  def getOnClause(cond: Expression): String = mustCompileExpr(cond)

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
