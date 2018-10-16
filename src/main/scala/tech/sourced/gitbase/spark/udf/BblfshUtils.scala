package tech.sourced.gitbase.spark.udf

import java.nio.ByteBuffer

import gopkg.in.bblfsh.sdk.v1.uast.generated.Node
import org.apache.spark.internal.Logging
import org.bblfsh.client.BblfshClient

import scala.collection.mutable.ArrayBuffer
import scala.util.{Try,Success,Failure}

object BblfshUtils extends Logging {
  /** Key used for the option to specify the host of the bblfsh grpc service. */
  val hostKey = "spark.tech.sourced.bblfsh.grpc.host"

  /** Key used for the option to specify the port of the bblfsh grpc service. */
  val portKey = "spark.tech.sourced.bblfsh.grpc.port"

  /** Default bblfsh host. */
  val defaultHost = "0.0.0.0"

  /** Default bblfsh port. */
  val defaultPort = 9432

  private var client: BblfshClient = _

  def getClient(): BblfshClient = synchronized {
    if (client == null) {
      val host = spark.conf.get(hostKey, defaultHost)
      val port = Try(spark.conf.get(portKey, defaultPort.toString).toInt) match {
        case Success(p) => p
        case Failure(e) => {
          log.warn(s"couldn't get value for config key ${portKey}, " +
            s"default value ${defaultPort} will be used")
          defaultPort
        }
      }
      client = BblfshClient(host, port)
    }
    client
  }

  def marshalNodes(nodes: Seq[Node]): Option[Array[Byte]] = {
    if (nodes == null || nodes.isEmpty) {
      None
    } else {
      val serialized = nodes.filter(_ != null).map(_.toByteArray)
      val size = serialized.foldLeft(0)((sum, sn) => sum + sn.length) + 4 * serialized.length
      val result = new Array[Byte](size)
      val buffer = ByteBuffer.wrap(result)
      serialized.foreach(sn => buffer.putInt(sn.length).put(sn))
      Some(result)
    }
  }

  def unmarshalNodes(blob: Array[Byte]): Option[Seq[Node]] = {
    if (blob == null || blob.isEmpty) {
      None
    } else {
      val nodes = ArrayBuffer[Node]()
      val buffer = ByteBuffer.wrap(blob)
      while (buffer.hasRemaining()) {
        val size = buffer.getInt()
        val sn = new Array[Byte](size)
        buffer.get(sn)
        val node = Node.parseFrom(sn)
        nodes += node
      }
      Some(nodes.toList)
    }
  }
}
