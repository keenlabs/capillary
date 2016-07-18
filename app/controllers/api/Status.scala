package controllers.api

import _root_.utils.JsonFormats._
import models.ZkKafka
import play.api.libs.json._
import play.api.mvc._

object Status extends Controller {

  def current(topoRoot: String, topic: String): Action[AnyContent] = Action {

    val totalsAndDeltas = ZkKafka.getTopologyDeltas(topoRoot, topic)

    Ok(Json.toJson(
      totalsAndDeltas._2
    ))
  }
}
