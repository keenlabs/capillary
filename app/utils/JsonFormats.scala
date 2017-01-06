package utils

import models.ZkKafka._
import play.api.libs.json.Json._
import play.api.libs.json._

object JsonFormats {

  private def optionLongtoJsValue(maybeId: Option[Long]) = maybeId.map({ l => JsNumber(l) }).getOrElse(JsNull)

  implicit object DeltaFormat extends Format[Delta] {

    def reads(json: JsValue): JsResult[Delta] = JsSuccess(Delta(
      partition   = (json \ "partition").as[Int],
      amount      = (json \ "amount").asOpt[Long],
      current     = (json \ "current").as[Long],
      storm       = (json \ "storm").asOpt[Long]
    ))

    def writes(o: Delta): JsValue = {

      val doc: Map[String,JsValue] = Map(
        "partition"      -> JsNumber(o.partition),
        "amount"         -> optionLongtoJsValue(o.amount),
        "current"        -> JsNumber(o.current),
        "storm"          -> optionLongtoJsValue(o.storm)
      )
      toJson(doc)
    }
  }
}
