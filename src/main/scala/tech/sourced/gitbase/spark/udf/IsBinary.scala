package tech.sourced.gitbase.spark.udf

import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object IsBinary extends CustomUDF with Logging {
  /** Name of the function. */
  override val name: String = "is_binary"

  /** Function to execute when this function is called. */
  override val function: UserDefinedFunction = udf(isBinary _)

  private val SniffLen = 8000

  /**
    * Detects if data is a binary value.
    * @see http://git.kernel.org/cgit/git/git.git/tree/xdiff-interface.c?id=HEAD#n198
    * @param data data to check
    * @return whether it's binary or not
    */
  def isBinary(data: Array[Byte]): Option[Boolean] = {
    if (data == null) {
      return None
    }

    val bytes = if (data.length > SniffLen) {
      data.take(SniffLen)
    } else {
      data
    }

    Some(bytes.contains(0))
  }

}

