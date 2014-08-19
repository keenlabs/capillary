package controllers

import models.ZkKafka
import models.ZkKafka._
import play.api._
import play.api.mvc._

object Application extends Controller {

  def index = Action { implicit request =>

    val topos = ZkKafka.getTopologies

    Ok(views.html.index(topos))
  }

  // def topo(name: String, topoRoot: String, topic: String) = Action { implicit request =>
  //   val stormState = ZkKafka.getSpoutState(topoRoot, topic)

  //   val zkState = ZkKafka.getKafkaState(topic)

    // var total = 0L;
    // val deltas = zkState.map({ partAndOffset =>
    //   val partition = partAndOffset._1
    //   val koffset = partAndOffset._2
    //   stormState.get(partition) map { soffset =>
    //     val amount = koffset - soffset
    //     total = amount + total
    //     Delta(partition = partition, amount = Some(amount), current = koffset, storm = Some(soffset))
    //   } getOrElse(
    //     Delta(partition = partition, amount = None, current = koffset, storm = None)
    //   )
    // }).toList.sortBy(_.partition)

    // Ok(views.html.topology(name, topic, total, deltas.toSeq))
  // }

}