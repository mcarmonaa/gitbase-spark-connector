package tech.sourced.gitbase.spark

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, NamedExpression, SortOrder}

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
                 source: Option[Node] = None,
                 sort: Seq[SortOrder] = Seq()) {

  /**
    * Creates a new query replacing the projected columns with the given ones.
    *
    * @param project given projected columns
    * @return new query
    */
  def withProject(project: Seq[Expression]): Query =
    Query(project, filters, source, sort)

  /**
    * Creates a new query adding the given filters to those already
    * present in the query.
    *
    * @param filters Given filters
    * @return new query
    */
  def withFilters(filters: Seq[Expression]): Query =
    Query(project, this.filters ++ filters, source, sort)

  /**
    * Creates a new query replacing the source with the given one.
    *
    * @param source source given
    * @return new query
    */
  def withSource(source: Node): Query =
    Query(project, filters, Some(source), sort)

  /**
    * Creates a new query replacing the sort fields with the given ones.
    *
    * @param sort sort fields given
    * @return new query
    */
  def withSort(sort: Seq[SortOrder]): Query =
    Query(project, filters, source, sort)

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
      transformUp {
        case Project(exprs, child) =>
          if (fields.isEmpty) {
            child
          } else {
            val newProjection = fields.flatMap(f => exprs.find {
              case e: NamedExpression => e.name == f.name
              case _ => false
            })

            if (fields.length != newProjection.length) {
              throw new SparkException("This is likely a bug, could not fit projection to schema. " +
                s"Schema: ${fields.map(_.name).mkString(", ")}, Projection: ${exprs.mkString(", ")}")
            }

            Project(newProjection, child)
          }
        case n => n
      }
    }
  }

  /**
    * Returns whether the tree contains any projection nodes.
    *
    * @return whether the tree contains projection nodes
    */
  def hasProjection: Boolean

  /**
    * Transforms the tree from the innermost node to the outermost by applying
    * the given function.
    *
    * @param fn transform function
    * @return transformed node
    */
  def transformUp(fn: Node => Node): Node
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

  override def transformUp(fn: Node => Node): Node = fn(this)
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

  override def transformUp(fn: Node => Node): Node =
    fn(Join(fn(left), fn(right), condition))
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

  override def transformUp(fn: Node => Node): Node =
    fn(Project(projection, fn(child)))
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

  override def transformUp(fn: Node => Node): Node =
    fn(Filter(filters, fn(child)))
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

  override def transformUp(fn: Node => Node): Node =
    fn(Sort(fields, fn(child)))
}

