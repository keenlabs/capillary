import java.lang.Runnable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit._
import models.ZkKafka
import play.api.Play.current
import play.api.{Application,GlobalSettings,Logger}

object Global extends GlobalSettings {

  final val metricsFetcherPool = Executors.newScheduledThreadPool(1)
  final val fetcher = new Runnable() {
    def run {
      ZkKafka.getTopologies.map({ topo =>
        Logger.info("Starting periodic metric collection")
        val totalAndDeltas = ZkKafka.getTopologyDeltas(topo.spoutRoot, topo.topic)
      })
    }
  }
  metricsFetcherPool.scheduleAtFixedRate(fetcher, 1, 5, SECONDS)

  override def onStart(app: Application) {
    Logger.info("Application has started")

  }

  override def onStop(app: Application) {
    Logger.info("Application is stopping, shutting down metrics thread")
    metricsFetcherPool.shutdown
  }
}