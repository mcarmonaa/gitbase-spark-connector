package tech.sourced.gitbase.spark

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

package object rule {

  def getAll: Seq[Rule[LogicalPlan]] = {
    List(
      AddSource,
      PushdownJoins
    )
  }

}
