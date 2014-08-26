package controllers

import com.codahale.metrics.json.MetricsModule
import com.codahale.metrics.{MetricRegistry, SharedMetricRegistries}
import com.fasterxml.jackson.databind.{ObjectWriter, ObjectMapper}
import java.io.StringWriter
import java.util.concurrent.TimeUnit
import models.Metrics
import models.ZkKafka
import models.ZkKafka._
import play.api.Play.current
import play.api._
import play.api.mvc._

object Application extends Controller {

  val validUnits = Some(Set("NANOSECONDS", "MICROSECONDS", "MILLISECONDS", "SECONDS", "MINUTES", "HOURS", "DAYS"))
  val mapper = new ObjectMapper()

  def registryName = Play.configuration.getString("metrics.name").getOrElse("default")
  def rateUnit     = Play.configuration.getString("metrics.rateUnit", validUnits).getOrElse("SECONDS")
  def durationUnit = Play.configuration.getString("metrics.durationUnit", validUnits).getOrElse("SECONDS")
  def showSamples  = Play.configuration.getBoolean("metrics.showSamples").getOrElse(false)

  val module = new MetricsModule(rateUnit, durationUnit, showSamples)
  mapper.registerModule(module)

  implicit def stringToTimeUnit(s: String) : TimeUnit = TimeUnit.valueOf(s)

  def index = Action { implicit request =>

    val topos = ZkKafka.getTopologies

    Ok(views.html.index(topos))
  }

  def topo(name: String, topoRoot: String, topic: String) = Action { implicit request =>
    val stormState = ZkKafka.getSpoutState(topoRoot, topic)

    val zkState = ZkKafka.getKafkaState(topic)

    var total = 0L;
    val deltas = zkState.map({ partAndOffset =>
      val partition = partAndOffset._1
      val koffset = partAndOffset._2
      stormState.get(partition) map { soffset =>
        val amount = koffset - soffset
        total = amount + total
        Delta(partition = partition, amount = Some(amount), current = koffset, storm = Some(soffset))
      } getOrElse(
        Delta(partition = partition, amount = None, current = koffset, storm = None)
      )
    }).toList.sortBy(_.partition)

    Ok(views.html.topology(name, topic, total, deltas.toSeq))
  }

  def metrics = Action {
    val writer: ObjectWriter = mapper.writerWithDefaultPrettyPrinter()
    val stringWriter = new StringWriter()
    writer.writeValue(stringWriter, Metrics.metricRegistry)
    Ok(stringWriter.toString).as("application/json").withHeaders("Cache-Control" -> "must-revalidate,no-cache,no-store")
  }
}