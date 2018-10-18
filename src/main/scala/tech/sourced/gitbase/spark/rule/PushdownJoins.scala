package tech.sourced.gitbase.spark.rule

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{StructField, StructType}
import tech.sourced.gitbase.spark._

object PushdownJoins extends Rule[LogicalPlan] {
  /** @inheritdoc */
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    // Joins are only applicable per repository, so we can push down completely
    // the join into the data source
    case q: Join =>
      val jd = JoinOptimizer.getJoinData(q)
      if (!jd.valid) {
        return q
      }

      jd match {
        case JoinData(Some(source), _, filters, projectExprs, attributes, servers, _) =>
          val node = DataSourceV2Relation(
            attributes,
            DefaultReader(
              servers,
              JoinOptimizer.attributesToSchema(attributes),
              source
            )
          )

          val filteredNode = filters match {
            case Some(filter) => Filter(filter, node)
            case None => node
          }

          // If the projection is empty, just return the filter
          if (projectExprs.nonEmpty) {
            Project(projectExprs, filteredNode)
          } else {
            filteredNode
          }
        case _ => q
      }

    // Remove two consecutive projects and replace it with the outermost one.
    case Project(list, Project(_, child)) =>
      Project(list, child)
  }
}

case class JoinData(source: Option[DataSource] = None,
                    conditions: Option[Expression] = None,
                    filter: Option[Expression] = None,
                    project: Seq[NamedExpression] = Nil,
                    attributes: Seq[AttributeReference] = Nil,
                    servers: Seq[GitbaseServer] = Nil,
                    valid: Boolean = false)

/**
  * Support methods for optimizing [[DefaultReader]]s.
  */
private[rule] object JoinOptimizer extends Logging {

  /**
    * Returns the data about a join to perform optimizations on it.
    *
    * @param j join to get the data from
    * @return join data
    */
  private[rule] def getJoinData(j: Join): JoinData = {
    // left and right ends in a GitRelation
    val leftRel = getGitbaseRelation(j.left)
    val rightRel = getGitbaseRelation(j.right)

    // Not a valid Join to optimize GitRelations
    if (leftRel.isEmpty || rightRel.isEmpty || !isJoinSupported(j)) {
      logUnableToOptimize("It doesn't have gitbase relations in both sides, " +
        "or the Join type is not supported.")
      return JoinData()
    }

    // Check Join conditions. They must be all conditions related with GitRelations
    val unsupportedConditions = JoinOptimizer.getUnsupportedConditions(
      j,
      leftRel.get,
      rightRel.get
    )

    if (unsupportedConditions.nonEmpty) {
      logUnableToOptimize(s"Obtained unsupported conditions: $unsupportedConditions")
      return JoinData()
    }

    j.condition match {
      case Some(cond) =>
        val tables = getRelationTables(leftRel.get, rightRel.get)
        if (!conditionsAllowPushdown(cond, tables)) {
          logUnableToOptimize("Join conditions are not restricted by repository_id")
          return JoinData()
        }
      case None =>
    }

    // Check if the Join contains all valid Nodes
    val jd: Seq[JoinData] = j.map {
      case jm@Join(_, _, _, condition) =>
        if (jm == j) {
          JoinData(conditions = condition, valid = true)
        } else {
          logUnableToOptimize(s"Invalid node: $jm")
          JoinData()
        }
      case Filter(cond, _) =>
        JoinData(filter = Some(cond), valid = true)
      case Project(namedExpressions, _) =>
        JoinData(project = namedExpressions, valid = true)
      case DataSourceV2Relation(out, DefaultReader(servers, _, source)) =>
        JoinData(Some(source), attributes = out, servers = servers, valid = true)
      case other =>
        logUnableToOptimize(s"Invalid node: $other")
        JoinData()
    }

    mergeJoinData(jd)
  }

  private def getRelationTables(left: DataSourceV2Relation,
                                right: DataSourceV2Relation): Seq[String] = {
    val leftSource = left.reader.asInstanceOf[DefaultReader].source
    val rightSource = right.reader.asInstanceOf[DefaultReader].source
    (getSourceTables(leftSource) ++ getSourceTables(rightSource)).distinct
  }

  private def getSourceTables(s: DataSource): Seq[String] = s match {
    case TableSource(t) => Seq(t)
    case JoinedSource(left, right, _) => (getSourceTables(left) ++ getSourceTables(right)).distinct
  }

  private def conditionsAllowPushdown(expression: Expression,
                                      tables: Seq[String]): Boolean = {
    expression.find {
      case EqualTo(left: Attribute, right: Attribute) =>
        val leftSource = left.metadata.getString(Sources.SourceKey)
        val rightSource = right.metadata.getString(Sources.SourceKey)
        left.name == "repository_id" && right.name == "repository_id" &&
          tables.contains(leftSource) && tables.contains(rightSource) &&
          leftSource != rightSource
      case And(left, right) =>
        conditionsAllowPushdown(left, tables) || conditionsAllowPushdown(right, tables)
      case _ => false
    }.isDefined
  }

  private def mergeAttributes(a: Seq[AttributeReference],
                              b: Seq[AttributeReference]): Seq[AttributeReference] = {
    val common = a.map(_.name).intersect(b.map(_.name))
    a ++ b.filter(attr => !common.contains(attr.name))
  }

  /**
    * Reduce all join data into one single join data.
    *
    * @param data sequence of join data to be merged
    * @return merged join data
    */
  private def mergeJoinData(data: Seq[JoinData]): JoinData = {
    val d = data.reduce((jd1, jd2) => {
      // get all filter expressions
      val filters: Option[Expression] = mixExpressions(
        jd1.filter,
        jd2.filter,
        And
      )
      // get all join conditions
      val conditions = mixExpressions(
        jd1.conditions,
        jd2.conditions,
        And
      )

      val source = (jd1.source, jd2.source) match {
        case (Some(s1), Some(s2)) => Some(JoinedSource(s1, s2, conditions))
        case (Some(s1), None) => Some(s1)
        case (None, Some(s1)) => Some(s1)
        case _ => None
      }

      JoinData(
        source,
        conditions,
        filters,
        jd1.project ++ jd2.project,
        mergeAttributes(jd1.attributes, jd2.attributes),
        (jd1.servers ++ jd2.servers).distinct,
        jd1.valid && jd2.valid
      )
    })

    if (d.source.isEmpty) {
      JoinData()
    } else {
      d
    }
  }

  private val supportedJoinTypes: Seq[JoinType] = Inner :: Nil

  /**
    * Reports whether the given join is supported.
    *
    * @param j join
    * @return is supported or not
    */
  def isJoinSupported(j: Join): Boolean = supportedJoinTypes.contains(j.joinType)

  /**
    * Retrieves all the unsupported conditions in the join.
    *
    * @param join  Join
    * @param left  left relation
    * @param right right relation
    * @return unsupported conditions
    */
  def getUnsupportedConditions(join: Join,
                               left: DataSourceV2Relation,
                               right: DataSourceV2Relation): Set[_] = {
    val leftReferences = left.references.baseSet
    val rightReferences = right.references.baseSet
    val joinReferences = join.references.baseSet
    joinReferences -- leftReferences -- rightReferences
  }

  /**
    * Mixes the two given expressions with the given join function if both exist
    * or returns the one that exists otherwise.
    *
    * @param l            left expression
    * @param r            right expression
    * @param joinFunction function used to join them
    * @return an optional expression
    */
  def mixExpressions(l: Option[Expression],
                     r: Option[Expression],
                     joinFunction: (Expression, Expression) => Expression):
  Option[Expression] = {
    (l, r) match {
      case (Some(expr1), Some(expr2)) => Some(joinFunction(expr1, expr2))
      case (None, None) => None
      case (le, None) => le
      case (None, re) => re
    }
  }

  /**
    * Creates a schema from a list of attributes.
    *
    * @param attributes list of attributes
    * @return resultant schema
    */
  def attributesToSchema(attributes: Seq[AttributeReference]): StructType =
    StructType(
      attributes
        .map((a: Attribute) => StructField(a.name, a.dataType, a.nullable, a.metadata))
        .toArray
    )

  /**
    * Returns the first git relation found in the given logical plan, if any.
    *
    * @param lp logical plan
    * @return git relation, or none if there is no such relation
    */
  def getGitbaseRelation(lp: LogicalPlan): Option[DataSourceV2Relation] =
    lp.find {
      case DataSourceV2Relation(_, _: DefaultReader) => true
      case _ => false
    } map(_.asInstanceOf[DataSourceV2Relation])

  private def logUnableToOptimize(msg: String = ""): Unit = {
    logError("*" * 80)
    logError("* This Join could not be optimized. This might severely impact the performance *")
    logError("* of your query. This happened because there is an unexpected node between the *")
    logError("* two relations of a Join, such as Limit or another kind of unknown relation.  *")
    logError("* Note that this will not stop your query or make it fail, only make it slow.  *")
    logError("*" * 80)
    if (msg.nonEmpty) {
      def split(str: String): Seq[String] = {
        if (str.length > 76) {
          Seq(str.substring(0, 76)) ++ split(str.substring(76))
        } else {
          Seq(str)
        }
      }

      logError(s"* Reason:${" " * 70}*")
      msg.lines.flatMap(split)
        .map(line => s"* $line${" " * (76 - line.length)} *")
        .foreach(logError(_))
      logError("*" * 80)
    }
  }

}
