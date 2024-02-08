package edu.polytechnique.cedar.spark.udao
import edu.polytechnique.cedar.spark.sql.component.F

import java.net.Socket
import java.io.{
  BufferedOutputStream,
  BufferedReader,
  DataOutputStream,
  InputStreamReader
}
import java.nio.{ByteBuffer, ByteOrder}
import java.time.Duration
class UdaoClient(
    host: String = "localhost",
    port: Int = 12345
) {
  private val socket = new Socket(host, port)

  private val out = new DataOutputStream(
    new BufferedOutputStream(socket.getOutputStream)
  )
  private val in = new BufferedReader(
    new InputStreamReader(socket.getInputStream, "UTF-8")
  )

  private def sendMsgWithLength(message: String): Unit = {
    val messageBytes = message.getBytes("UTF-8")
    val lengthBytes = ByteBuffer
      .allocate(4)
      .order(ByteOrder.BIG_ENDIAN)
      .putInt(messageBytes.length)
      .array()
    out.write(lengthBytes ++ messageBytes)
    out.flush()
  }

  def getUpdateTheta(message: String): (String, Duration) = F.runtime {
    sendMsgWithLength(message)
    in.readLine()
  }

  def close(): Unit = socket.close()

}
