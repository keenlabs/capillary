import org.specs2.mutable._
import org.specs2.runner._
import org.junit.runner._

import play.api.test._
import play.api.test.Helpers._

import models.ZkKafka

class BasicSpec extends Specification {
  "ZkKafka model" should {
    "make paths correctly" in new WithApplication(FakeApplication()) {
      ZkKafka.makePath(Seq(None, Some("poop"), None, Some("fart"))) must beEqualTo("/poop/fart")
    }
  }
}