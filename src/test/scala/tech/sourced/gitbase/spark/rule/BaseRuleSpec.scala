package tech.sourced.gitbase.spark.rule

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructType}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, Suite}
import tech.sourced.gitbase.spark.{DefaultReader, Node, Sources}

trait BaseRuleSpec extends FlatSpec with Matchers with BeforeAndAfterAll with Logging {
  this: Suite =>

  def attr(name: String, typ: DataType, table: String = "", id: Long = 0): AttributeReference = {
    val metadata = if (table.isEmpty) {
      new MetadataBuilder().build()
    } else {
      new MetadataBuilder().putString(Sources.SourceKey, table).build()
    }
    AttributeReference(name, typ, nullable = false, metadata)(exprId = if (id <= 0) {
      NamedExpression.newExprId
    } else {
      ExprId(id)
    })
  }

  def reader(node: Node, schema: StructType = StructType(Seq())): DefaultReader = {
    DefaultReader(Seq(), schema, node)
  }

  def expressionsMatch(a: Seq[Expression], b: Seq[Expression]): Boolean = {
    if (a.length != b.length) {
      return false
    }

    for (i <- a.indices) {
      if (!expressionMatch(a(i), b(i))) {
        return false
      }
    }

    true
  }

  def expressionMatch(a: Expression, b: Expression): Boolean = {
    (a, b) match {
      case (AttributeReference(n1, _, _, _), AttributeReference(n2, _, _, _)) =>
        n1 == n2
      case (_a, _b) => _a.getClass.equals(_b.getClass) &&
        expressionsMatch(_a.children, _b.children)
    }
  }

}
