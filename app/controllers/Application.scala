package controllers

import models.ZkKafka
import models.ZkKafka._
import play.api._
import play.api.mvc._

object Application extends Controller {

  def index(topoRoot: String, topic: String) = Action {

    val stormState = ZkKafka.getSpoutState(topoRoot, topic)

    val zkState = ZkKafka.getKafkaState(topic)

    val deltas = zkState map { partAndOffset =>
      val partition = partAndOffset._1
      val koffset = partAndOffset._2
      stormState.get(partition) map { soffset =>
        Delta(partition = partition, amount = Some(koffset - soffset), current = koffset, storm = Some(soffset))
      } getOrElse(
        Delta(partition = partition, amount = None, current = koffset, storm = None)
      )
    }

    Ok(views.html.index(topic, deltas.toSeq))
  }

}