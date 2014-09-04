package controllers.api

import models.ZkKafka
import models.ZkKafka._
import play.api._
import play.api.libs.json._
import play.api.mvc._
import _root_.utils.JsonFormats._

object Status extends Controller {

  def current(topoRoot: String, topic: String) = Action {

    val stormState = ZkKafka.getSpoutState(topoRoot, topic)

    val zkState = ZkKafka.getKafkaState(topic)

    val totalAndDeltas = ZkKafka.getTopologyDeltas(topoRoot, topic)

    Ok(Json.toJson(
      totalAndDeltas._2.toSeq
    ))
  }
}