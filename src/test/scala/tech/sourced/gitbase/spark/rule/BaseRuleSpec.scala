package tech.sourced.gitbase.spark.rule

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructType}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, Suite}
import tech.sourced.gitbase.spark.{DefaultReader, Node, Sources}

trait BaseRuleSpec extends FlatSpec with Matchers with BeforeAndAfterAll with Logging {
  this: Suite =>

  def attr(name: String, typ: DataType, table: String = ""): AttributeReference = {
    val metadata = if (table.isEmpty) {
      new MetadataBuilder().build()
    } else {
      new MetadataBuilder().putString(Sources.SourceKey, table).build()
    }
    AttributeReference(name, typ, nullable=false, metadata)()
  }

  def reader(node: Node, schema: StructType = StructType(Seq())): DefaultReader=  {
    DefaultReader(Seq(), schema, node)
  }

}
