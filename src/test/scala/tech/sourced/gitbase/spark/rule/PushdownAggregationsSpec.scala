package tech.sourced.gitbase.spark.rule

import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Alias, CaseKeyWhen, Divide, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import tech.sourced.gitbase.spark.{DefaultReader, GroupBy, Table}

class PushdownAggregationsSpec extends BaseRuleSpec {
  behavior of "PushdownAggregations"

  private val colA = attr("a", StringType, "foo", 1)
  private val colB = attr("b", StringType, "foo", 1)

  private val relation = DataSourceV2Relation(
    Seq(
      colA,
      colB
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
    colA,
    Seq(
      colA,
      colB
    )
  )

  val testCases = Seq(
    AggregationTestCase(
      "push down count aggregation",
      Seq(
        Alias(Count(Seq(colA)), "count")(),
        colA
      ),
      Seq(
        Alias(Sum(attr("count(a#1)", IntegerType)), "count")(),
        colA
      ),
      Seq(
        colA,
        Alias(Count(Seq(colA)), "count(a#1)")(),
        colB
      )
    ),

    AggregationTestCase(
      "push down sum aggregation",
      Seq(
        Alias(Sum(colA), "sum")(),
        colA
      ),
      Seq(
        Alias(Sum(attr("sum(a#1)", IntegerType)), "sum")(),
        colA
      ),
      Seq(
        colA,
        Alias(Sum(colA), "sum(a#1)")(),
        colB
      )
    ),

    AggregationTestCase(
      "push down min aggregation",
      Seq(
        Alias(Min(colA), "min")(),
        colA
      ),
      Seq(
        Alias(Min(attr("min(a#1)", IntegerType)), "min")(),
        colA
      ),
      Seq(
        colA,
        Alias(Min(colA), "min(a#1)")(),
        colB
      )
    ),

    AggregationTestCase(
      "push down max aggregation",
      Seq(
        Alias(Max(colA), "max")(),
        colA
      ),
      Seq(
        Alias(Max(attr("max(a#1)", IntegerType)), "min")(),
        colA
      ),
      Seq(
        colA,
        Alias(Max(colA), "max(a#1)")(),
        colB
      )
    ),

    AggregationTestCase(
      "push down avg aggregation",
      Seq(
        Alias(Average(colA), "avg")(),
        colA
      ),
      Seq(
        Alias(Divide(
          attr("sum(a#1)", IntegerType),
          attr("count(a#1)", IntegerType)
        ), "avg")(),
        colA
      ),
      Seq(
        colA,
        Alias(Count(colA), "count(a#1)")(),
        Alias(Sum(colA), "sum(a#1)")(),
        colB
      )
    )
  )

  testCases.foreach(test => {
    it should test.name in {
      val node = Aggregate(
        Seq(
          colA,
          colB
        ),
        test.inputAggregate,
        relation
      )

      val result = PushdownAggregations(node)
      result.isInstanceOf[Aggregate] should be(true)
      val agg = result.asInstanceOf[Aggregate]
      expressionsMatch(
        agg.aggregateExpressions,
        test.resultAggregate
      ) should be(true)

      expressionsMatch(
        agg.groupingExpressions,
        Seq(colA, colB)
      ) should be(true)

      agg.child.isInstanceOf[DataSourceV2Relation] should be(true)

      val rel = agg.child.asInstanceOf[DataSourceV2Relation]
      val query = rel.reader.asInstanceOf[DefaultReader].node
      query.isInstanceOf[GroupBy] should be(true)

      val groupBy = query.asInstanceOf[GroupBy]

      expressionsMatch(
        groupBy.aggregate,
        test.pushedDownAggregate
      ) should be(true)

      expressionsMatch(
        groupBy.grouping,
        Seq(colA, colB)
      ) should be(true)
    }
  })
}

case class AggregationTestCase(name: String,
                               inputAggregate: Seq[NamedExpression],
                               resultAggregate: Seq[NamedExpression],
                               pushedDownAggregate: Seq[NamedExpression])
