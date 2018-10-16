package tech.sourced.gitbase.spark

import java.sql._

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

case class GitbaseServer(host: String, user: String, password: String)

/**
  * Gitbase dialect is a JdbcDialect for Gitbase.
  *
  * @param protocol protocol to use, "jdbc:mariadb" by default.
  */
case class GitbaseDialect(protocol: String = "jdbc:mariadb") extends JdbcDialect {

  override def canHandle(url: String): Boolean = url.startsWith(protocol)

  override def compileValue(value: Any): Any = value match {
    case v: UTF8String => s"'${escapeSql(v.toString)}'"
    case v: Seq[Any] => v.map(compileValue).mkString(", ")
    case v: Boolean => if (v) 1 else 0
    case _ => super.compileValue(value)
  }

}

/**
  * This contains utility methods to perform operations on a Gitbase server.
  */
object Gitbase {

  private val dialect = GitbaseDialect()

  /**
    * Takes a (schema, table) specification and returns the table's Catalyst
    * schema.
    *
    * @param server gitbase server details.
    * @param table table name.
    * @return A StructType giving the table's Catalyst schema.
    * @throws SQLException if the table specification is garbage.
    * @throws SQLException if the table contains an unsupported type.
    */
  def resolveTable(server: GitbaseServer, table: String): StructType = {
    val options = new JDBCOptions(
      s"jdbc:mariadb://${server.host}",
      table,
      Map(
        "driver" -> "org.mariadb.jdbc.Driver",
        "user" -> server.user,
        "password" -> server.password
      )
    )
    val conn: Connection = JdbcUtils.createConnectionFactory(options)()
    try {
      val statement = conn.prepareStatement(dialect.getSchemaQuery(table))
      try {
        val rs = statement.executeQuery()
        try {
          Sources.addToSchema(
            JdbcUtils.getSchema(rs, dialect, alwaysNullable = true),
            table
          )
        } finally {
          rs.close()
        }
      } finally {
        statement.close()
      }
    } finally {
      conn.close()
    }
  }

  /**
    * Connects using the given connection string and executes a query.
    *
    * @param server data to connect to the gitbase server
    * @param query            query to execute
    * @return iterator of rows and a closure to close the connection after
    *         the iterator has been used.
    */
  def query(server: GitbaseServer, query: String): (Iterator[Row], () => Unit) = {
    val connection = DriverManager.getConnection(
      s"jdbc:mariadb://${server.host}",
      server.user,
      server.password
    )

    val stmt: PreparedStatement = connection.prepareStatement(query)
    try {
      val rs = stmt.executeQuery

      val schema = JdbcUtils.getSchema(rs, dialect, alwaysNullable = true)
      (JdbcUtils.resultSetToRows(rs, schema), connection.close)
    } finally {
      stmt.close()
    }
  }

}
