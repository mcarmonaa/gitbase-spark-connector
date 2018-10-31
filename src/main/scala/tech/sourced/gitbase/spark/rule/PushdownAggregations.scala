package tech.sourced.gitbase.spark.rule

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Cast, Divide, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.IntegerType
import tech.sourced.gitbase.spark._

object PushdownAggregations extends Rule[LogicalPlan] {

  /** @inheritdoc */
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    // Ignore aggregates with no aggregate expressions.
    case n@logical.Aggregate(_, Nil, _) => n

    case n@logical.Aggregate(grouping, aggregate,
    DataSourceV2Relation(_, DefaultReader(servers, _, query))) =>
      if (!canBeHandled(grouping) || !canBeHandled(aggregate) || containsGroupBy(query)) {
        return fixAttributeReferences(n)
      }

      val transformedAggregate = aggregate.flatMap(_.flatMap {
        case e@(_: Count | _: Min | _: Max | _: Sum) => Seq(e)
        case Average(child) => Seq(Count(child), Sum(child))
        case r: AttributeReference => Seq(r)
        case _ => Seq()
      }).map {
        case n: NamedExpression => n
        case e => Alias(e, e.toString())()
      }.groupBy(_.name).values.map(_.head).toSeq.sortBy(_.name)

      val missingAttrs = grouping.flatMap(_.flatMap {
        case a: AttributeReference =>
          if (transformedAggregate.exists(n => n.name == a.name)) {
            None
          } else {
            Some(a)
          }
        case _ => None
      }).groupBy(_.name).values.map(_.head).toSeq.sortBy(_.name)

      val pushedDownAggregate = transformedAggregate ++ missingAttrs

      val newOut = pushedDownAggregate.map {
        // FIXME: hack to make it work with gitbase. Gitbase emits counts as INTEGER
        // but spark wants longs.
        case e@Alias(Count(_), _) =>
          AttributeReference(e.name, IntegerType, e.nullable, e.metadata)()
        case e =>
          AttributeReference(e.name, e.dataType, e.nullable, e.metadata)()
      }

      val newAggregate = aggregate.map(e => e.transformUp {
        case n@Count(child) =>
          val cnt = newOut.find(_.name == Count(child.head).toString)
            .getOrElse(throw new SparkException("This is likely a bug. Could not find matching COUNT" +
              s" to be pushed down for COUNT(${child.head})"))
          Sum(cnt)

        case Average(child) =>
          val sum = newOut.find(_.name == Sum(child).toString)
            .getOrElse(throw new SparkException("This is likely a bug. Could not find matching SUM" +
              s" to be pushed down for AVG($child)"))
          val cnt = newOut.find(_.name == Count(child).toString)
            .getOrElse(throw new SparkException("This is likely a bug. Could not find matching COUNT" +
              s" to be pushed down for AVG($child)"))
          Divide(sum, cnt)

        case n@Min(child) =>
          val min = newOut.find(_.name == n.toString)
            .getOrElse(throw new SparkException("This is likely a bug. Could not find matching MIN" +
              s" to be pushed down for attribute MIN($child)"))
          Min(min)

        case n@Max(child) =>
          val max = newOut.find(_.name == n.toString)
            .getOrElse(throw new SparkException("This is likely a bug. Could not find matching MAX" +
              s" to be pushed down for attribute MAX($child)"))
          Max(max)

        case n@Sum(child) =>
          val sum = newOut.find(_.name == n.toString)
            .getOrElse(throw new SparkException("This is likely a bug. Could not find matching SUM" +
              s" to be pushed down for attribute SUM($child)"))
          Sum(sum)

        case expr => expr
      }.asInstanceOf[NamedExpression])

      logical.Aggregate(
        grouping,
        newAggregate,
        DataSourceV2Relation(
          newOut,
          DefaultReader(
            servers,
            attributesToSchema(newOut),
            GroupBy(pushedDownAggregate, grouping, query)
          )
        )
      )

    case node: DataSourceV2Relation => node

    case node => fixAttributeReferences(node)
  }

  private def canBeHandled(exprs: Seq[Expression]): Boolean = {
    exprs.flatMap(x => QueryBuilder.compileExpression(x)).length == exprs.length
  }

}
