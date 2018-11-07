package tech.sourced.gitbase.spark.rule

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.MetadataBuilder
import tech.sourced.gitbase.spark.{DefaultReader, Sources, Table}

/**
  * Rule to assign to an
  * [[org.apache.spark.sql.catalyst.expressions.AttributeReference]]
  * metadata to identify the table it belongs to.
  */
object AddSource extends Rule[LogicalPlan] {

  /** @inheritdoc */
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case DataSourceV2Relation(output, reader@DefaultReader(_, _, Table(table))) =>
      val metadata = new MetadataBuilder().putString(Sources.SourceKey, table).build()
      val out = output.map(_.withMetadata(metadata).asInstanceOf[AttributeReference])
      DataSourceV2Relation(out, reader)
  }

}
