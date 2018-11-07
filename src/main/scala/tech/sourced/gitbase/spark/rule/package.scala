package tech.sourced.gitbase.spark

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

package object rule {

  def getAll: Seq[Rule[LogicalPlan]] = {
    List(
      AddSource,
      PushdownJoins,
      PushdownTree
    )
  }

  private[rule] def fixAttributeReferences(plan: LogicalPlan): LogicalPlan = {
    import JoinOptimizer._
    val availableAttrs: Seq[Attribute] = plan.children.flatMap(child => {
      child.find {
        case _: logical.Project => true
        case DataSourceV2Relation(_, _: DefaultReader) => true
        case _: logical.Join => true
        case _ => false
      } match {
        case Some(logical.Project(attrs, _)) => attrs.map(_.toAttribute)
        case Some(DataSourceV2Relation(output, _: DefaultReader)) => output
        case _ => Seq()
      }
    })

    plan.transformExpressionsUp {
      case a: Attribute =>
        val candidates = availableAttrs.filter(attr => attr.name == a.name)
        if (candidates.nonEmpty) {
          candidates.find(attr => getSource(a) == getSource(attr))
              .getOrElse(candidates.head)
        } else {
          a
        }
      case x => x
    }
  }

}
