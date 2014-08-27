import play.api.Play.current
import play.api.{Application,GlobalSettings,Logger}

object Global extends GlobalSettings {

  override def onStart(app: Application) {
    Logger.info("Application has started")
  }
}