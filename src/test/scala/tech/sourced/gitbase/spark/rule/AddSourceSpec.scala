package tech.sourced.gitbase.spark.rule

import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.StringType
import tech.sourced.gitbase.spark.{Sources, Table}

class AddSourceSpec extends BaseRuleSpec {
  behavior of "AddSource"

  it should "add sources to relation attributes" in {
    val attrs = Seq(
      attr("a", StringType),
      attr("b", StringType),
      attr("c", StringType)
    )
    val node = DataSourceV2Relation(attrs, reader(Table("foo")))

    val result = AddSource(node)
    val output = result.asInstanceOf[DataSourceV2Relation].output
    output.length should be(attrs.length)
    for (i <- attrs.indices) {
      output(i).name should be(attrs(i).name)
      output(i).metadata.getString(Sources.SourceKey) should be("foo")
    }
  }
}
