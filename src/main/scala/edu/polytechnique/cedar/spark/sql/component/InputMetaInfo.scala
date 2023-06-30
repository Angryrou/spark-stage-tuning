package edu.polytechnique.cedar.spark.sql.component
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render

case class InputMetaInfo(inputSizeInBytes: BigInt, inputRowCount: BigInt)
    extends MyUnit {
  private val json =
    ("inputSizeInBytes" -> inputSizeInBytes) ~ ("inputRowCount" -> inputRowCount)

  override def toJson: JValue = render(json)
}
