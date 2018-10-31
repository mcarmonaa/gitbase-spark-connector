package tech.sourced.gitbase.spark

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference,
  Expression,
  NamedExpression,
  SortOrder
}

/**
  * Query holds all the data needed to build a query.
  *
  * @param project Projected columns
  * @param filters Filters
  * @param source  Source of the query (table or join between tables)
  * @param sort    Sorting by fields
  */
case class Query(project: Seq[Expression] = Seq(),
                 filters: Seq[Expression] = Seq(),
                 subquery: Option[Query] = None,
                 source: Option[Node] = None,
                 sort: Seq[SortOrder] = Seq(),
                 grouping: Seq[Expression] = Seq()) {

  private def removeSources(e: Seq[Expression]): Seq[Expression] =
    e.map(_.transformUp {
      case x: AttributeReference =>
        AttributeReference(x.name, x.dataType, x.nullable)()
    })

  private def removeSourcesFromSort(e: Seq[SortOrder]): Seq[SortOrder] =
    e.map(s => {
      val child = s.child.transformUp {
        case x: AttributeReference =>
          AttributeReference(x.name, x.dataType, x.nullable)()
      }
      SortOrder(child, s.direction)
    })

  private def withoutSources: Query = {
    Query(
      removeSources(project),
      removeSources(filters),
      subquery,
      source,
      removeSourcesFromSort(sort),
      removeSources(grouping)
    )
  }

  /**
    * Creates a new query replacing the projected columns with the given ones.
    * If there is already a projection, it will be treated as a subquery.
    *
    * @param project given projected columns
    * @return new query
    */
  def withProject(project: Seq[Expression]): Query =
    if (this.project.isEmpty) {
      Query(project, filters, subquery, source, sort, grouping)
    } else {
      Query(project, subquery = Some(this)).withoutSources
    }

  /**
    * Creates a new query replacing the grouping columns and the projection
    * with the given ones. If there is already a projection, it will wrap the
    * current query, making it a subquery.
    *
    * @param project  projection columns
    * @param grouping columns
    * @return new query
    */
  def withGroupBy(project: Seq[Expression], grouping: Seq[Expression]): Query =
    if (this.project.isEmpty) {
      Query(project, filters, subquery, source, sort, grouping)
    } else {
      Query(project = project, subquery = Some(this), grouping = grouping)
        .withoutSources
    }

  /**
    * Creates a new query adding the given filters to those already
    * present in the query.
    *
    * @param filters Given filters
    * @return new query
    */
  def withFilters(filters: Seq[Expression]): Query = {
    val f = if (subquery.isDefined) {
      removeSources(filters)
    } else {
      filters
    }
    Query(project, this.filters ++ f, subquery, source, sort, grouping)
  }

  /**
    * Creates a new query replacing the source with the given one.
    *
    * @param source source given
    * @return new query
    */
  def withSource(source: Node): Query =
    Query(project, filters, subquery, Some(source), sort, grouping)

  /**
    * Creates a new query replacing the sort fields with the given ones.
    *
    * @param sort sort fields given
    * @return new query
    */
  def withSort(sort: Seq[SortOrder]): Query = {
    val s = if (subquery.isDefined) {
      removeSourcesFromSort(sort)
    } else {
      sort
    }
    Query(project, filters, subquery, source, s, grouping)
  }

}

/**
  * Node of a query.
  */
sealed trait Node {
  /**
    * Given a query, it builds another query with more data.
    */
  def buildQuery(q: Query): Query

  /**
    * Makes the tree fit a certain schema by changing the projected columns
    * or adding a new Project node if there is no projection.
    *
    * @param fields fields to project
    * @return new node adapted to the given schema
    */
  def fitSchema(fields: Seq[AttributeReference]): Node = {
    if (!hasProjection && fields.nonEmpty) {
      Project(fields, this)
    } else {
      transformSingleDown {
        case Project(exprs, child) =>
          if (fields.isEmpty) {
            Some(child)
          } else {
            val newProjection = fields.flatMap(f => exprs.find {
              case e: NamedExpression => e.name == f.name
              case _ => false
            })

            if (fields.length != newProjection.length) {
              throw new SparkException("This is likely a bug, could not fit projection to " +
                s"schema. Schema: ${fields.map(_.name).mkString(", ")}, " +
                s"Projection: ${exprs.mkString(", ")}")
            }

            Some(Project(newProjection, child))
          }
        case GroupBy(agg, grouping, child) =>
          if (agg.isEmpty) {
            Some(child)
          } else {
            val newProjection = fields.flatMap(f => agg.find {
              case e: NamedExpression => e.name == f.name
              case _ => false
            })

            if (fields.length != newProjection.length) {
              throw new SparkException("This is likely a bug, could not fit group by to " +
                s"schema. Schema: ${fields.map(_.name).mkString(", ")}, " +
                s"Aggregation: ${agg.mkString(", ")}")
            }

            Some(GroupBy(newProjection, grouping, child))
          }
        case n => None
      }
    }.getOrElse(this)
  }

  /**
    * Returns whether the tree contains any projection nodes.
    *
    * @return whether the tree contains projection nodes
    */
  def hasProjection: Boolean

  /**
    * Transforms the tree from the outermost node to the innermost until a
    * single node is transformed, then stops.
    *
    * @param fn transform function
    * @return transformed node, if any.
    */
  def transformSingleDown(fn: Node => Option[Node]): Option[Node]
}

/**
  * Table node.
  *
  * @param name name of the table
  */
case class Table(name: String) extends Node {
  override def buildQuery(q: Query): Query =
    q.withSource(this)

  override def hasProjection: Boolean = false

  override def transformSingleDown(fn: Node => Option[Node]): Option[Node] = fn(this)
}

/**
  * Join between two nodes.
  *
  * @param left      left node
  * @param right     right node
  * @param condition a condition if it's an inner join
  */
case class Join(left: Node, right: Node, condition: Option[Expression]) extends Node {
  override def buildQuery(q: Query): Query =
    q.withSource(this)

  override def hasProjection: Boolean = left.hasProjection || right.hasProjection

  override def transformSingleDown(fn: Node => Option[Node]): Option[Node] = {
    fn(this).orElse(
      fn(left).map(x => Join(x, right, condition)).orElse(
        fn(right).map(x => Join(left, x, condition))
      )
    )
  }
}

/**
  * Projection of expressions from the child node.
  *
  * @param projection projected expressions
  * @param child      child node
  */
case class Project(projection: Seq[Expression], child: Node) extends Node {
  override def buildQuery(q: Query): Query =
    child.buildQuery(q).withProject(projection)

  override def hasProjection: Boolean = true

  override def transformSingleDown(fn: Node => Option[Node]): Option[Node] = {
    fn(this).orElse(
      fn(child).map(x => Project(projection, x))
    )
  }
}

/**
  * Filter of the child node.
  *
  * @param filters filters
  * @param child   child node
  */
case class Filter(filters: Seq[Expression], child: Node) extends Node {
  override def buildQuery(q: Query): Query =
    child.buildQuery(q).withFilters(filters)

  override def hasProjection: Boolean = child.hasProjection

  override def transformSingleDown(fn: Node => Option[Node]): Option[Node] = {
    fn(this).orElse(
      fn(child).map(x => Filter(filters, x))
    )
  }
}

/**
  * Ordering of the child node.
  *
  * @param fields sort fields
  * @param child  child node
  */
case class Sort(fields: Seq[SortOrder], child: Node) extends Node {
  override def buildQuery(q: Query): Query =
    child.buildQuery(q).withSort(fields)

  override def hasProjection: Boolean = child.hasProjection

  override def transformSingleDown(fn: Node => Option[Node]): Option[Node] = {
    fn(this).orElse(
      fn(child).map(x => Sort(fields, x))
    )
  }
}

/**
  * Aggregation the child node.
  *
  * @param aggregate aggregate columns
  * @param grouping  grouping columns
  * @param child     child node
  */
case class GroupBy(aggregate: Seq[Expression],
                   grouping: Seq[Expression],
                   child: Node) extends Node {
  override def buildQuery(q: Query): Query =
    child.buildQuery(q).withGroupBy(aggregate, grouping)

  override def hasProjection: Boolean = true

  override def transformSingleDown(fn: Node => Option[Node]): Option[Node] = {
    fn(this).orElse(
      fn(child).map(x => GroupBy(aggregate, grouping, x))
    )
  }
}

