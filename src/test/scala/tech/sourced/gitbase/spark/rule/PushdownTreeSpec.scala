package tech.sourced.gitbase.spark.rule

import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Ascending,
  Attribute,
  CaseKeyWhen,
  Descending,
  EqualTo,
  SortOrder
}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import tech.sourced.gitbase.spark._

class PushdownTreeSpec extends BaseRuleSpec {
  behavior of "PushdownTree"

  private val relation = DataSourceV2Relation(
    Seq(
      attr("a", StringType, "foo"),
      attr("b", StringType, "foo")
    ),
    reader(
      Table("foo"),
      StructType(Seq(
        StructField("a", StringType),
        StructField("b", StringType)
      ))
    )
  )

  private val unsupported = CaseKeyWhen(
    attr("a", StringType, "foo"),
    Seq(
      attr("b", StringType, "foo"),
      attr("a", StringType, "foo")
    )
  )

  it should "not push down a project with duplicate attributes" in {
    val node = logical.Project(
      Seq(
        attr("a", StringType, "foo"),
        attr("b", StringType, "foo"),
        attr("a", StringType, "foo")
      ),
      relation
    )

    val result = PushdownTree(node)
    result.isInstanceOf[logical.Project] should be(true)
  }

  it should "push down a project" in {
    val node = logical.Project(
      Seq(
        attr("a", StringType, "foo"),
        attr("b", StringType, "foo")
      ),
      relation
    )

    val result = PushdownTree(node)
    result.isInstanceOf[DataSourceV2Relation] should be(true)
    val rel = result.asInstanceOf[DataSourceV2Relation]
    rel.output.map(_.name) should equal(Seq("a", "b"))
    val reader = rel.reader.asInstanceOf[DefaultReader]
    reader.node.isInstanceOf[Project] should be(true)
    val project = reader.node.asInstanceOf[Project]
    project.projection.map(_.asInstanceOf[Attribute].name) should equal(Seq("a", "b"))
    project.child should equal(Table("foo"))
  }

  it should "push down filters" in {
    val node = logical.Filter(
      EqualTo(
        attr("a", StringType, "foo"),
        attr("b", StringType, "foo")
      ),
      relation
    )

    val result = PushdownTree(node)
    result.isInstanceOf[DataSourceV2Relation] should be(true)
    val reader = result.asInstanceOf[DataSourceV2Relation].reader.asInstanceOf[DefaultReader]
    reader.node.isInstanceOf[Filter] should be(true)
    val filter = reader.node.asInstanceOf[Filter]
    filter.filters should be(Seq(node.condition))
    filter.child should equal(Table("foo"))
  }

  it should "push down local sorts" in {
    val node = logical.Sort(
      Seq(
        SortOrder(attr("a", StringType, "foo"), Ascending),
        SortOrder(attr("b", StringType, "foo"), Descending)
      ),
      global = false,
      relation
    )

    val result = PushdownTree(node)
    result.isInstanceOf[DataSourceV2Relation] should be(true)
    val reader = result.asInstanceOf[DataSourceV2Relation].reader.asInstanceOf[DefaultReader]
    reader.node.isInstanceOf[Sort] should be(true)
    val sort = reader.node.asInstanceOf[Sort]
    sort.fields should be(node.order)
    sort.child should equal(Table("foo"))
  }

  it should "not push down global sorts" in {
    val node = logical.Sort(
      Seq(
        SortOrder(attr("a", StringType, "foo"), Ascending),
        SortOrder(attr("b", StringType, "foo"), Descending)
      ),
      global = true,
      relation
    )

    val result = PushdownTree(node)
    result.isInstanceOf[logical.Sort] should be(true)
  }

  it should "fix expressions of nodes that are not pushed down" in {
    val node = logical.Project(
      Seq(
        attr("a", StringType, "foo"),
        attr("b", StringType, "foo"),
        attr("b", StringType, "foo")
      ),
      logical.Project(
        Seq(
          attr("a", StringType, "foo"),
          attr("b", StringType, "foo")
        ),
        relation
      )
    )

    val result = PushdownTree(node)
    val outerProject = result.asInstanceOf[logical.Project]
    val rel = outerProject.child.asInstanceOf[DataSourceV2Relation]

    outerProject.projectList.take(rel.output.length) should equal(rel.output)
  }

  it should "not push down project with unsupported expressions" in {
    val node = logical.Project(
      Seq(Alias(unsupported, "x")()),
      relation
    )

    PushdownTree(node).isInstanceOf[logical.Project] should be(true)
  }

  it should "not push down filter with unsupported expressions" in {
    val node = logical.Filter(
      unsupported,
      relation
    )

    PushdownTree(node).isInstanceOf[logical.Filter] should be(true)
  }

  it should "not push down sort with unsupported expressions" in {
    val node = logical.Sort(
      Seq(SortOrder(unsupported, Ascending)),
      global = false,
      relation
    )

    PushdownTree(node).isInstanceOf[logical.Sort] should be(true)
  }
}
