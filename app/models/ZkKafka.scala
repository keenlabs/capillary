package models

import com.twitter.zookeeper.ZooKeeperClient
import kafka.api.{OffsetFetchRequest,OffsetRequest,PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import play.api.libs.json._
import play.api.Play
import play.api.Play.current

object ZkKafka {

  case class Delta(partition: Int, amount: Option[Long], current: Long, storm: Option[Long])
  case class Topology(name: String, spoutRoot: String, topic: String)

  def topoCompFn(t1: Topology, t2: Topology) = {
    (t1.name compareToIgnoreCase t2.name) < 0
  }

  val zookeepers = Play.configuration.getString("capillary.zookeepers").getOrElse("localhost:2181")
  val kafkaZkRoot = Play.configuration.getString("capillary.kafka.zkroot")
  val stormZkRoot = Play.configuration.getString("capillary.storm.zkroot")
  lazy val zk = new ZooKeeperClient(zookeepers)

  def makePath(parts: Seq[Option[String]]): String = {
    parts.foldLeft("")({ (path, maybeP) => maybeP.map({ p => path + "/" + p }).getOrElse(path) })
  }

  def getTopologies: Seq[Topology] = {
    zk.getChildren(makePath(Seq(stormZkRoot))).map({ r =>
      getSpoutTopology(r)
    }).sortWith(topoCompFn)
  }

  def getSpoutTopology(root: String): Topology = {
    val s = zk.getChildren(makePath(Seq(stormZkRoot, Some(root))))
    val parts = zk.getChildren(makePath(Seq(stormZkRoot, Some(root), Some(s(0)))))
    val jsonState = new String(zk.get(makePath(Seq(stormZkRoot, Some(root), Some(s(0)), Some(parts(0))))))
    val state = Json.parse(jsonState)
    val topic = (state \ "topic").as[String]
    val name = (state \ "topology" \ "name").as[String]
    Topology(name = name, topic = topic, spoutRoot = root)
  }

  def getSpoutState(root: String, topic: String): Map[Int, Long] = {
    // There is basically nothing for error checking in here.

    val s = zk.getChildren(makePath(Seq(stormZkRoot, Some(root))))

    // We assume that there's only one child. This might break things
    val parts = zk.getChildren(makePath(Seq(stormZkRoot, Some(root), Some(s(0)))))

    return parts.map({ vp =>
      val jsonState = new String(zk.get(makePath(Seq(stormZkRoot, Some(root), Some(s(0)), Some(vp)))))
      val state = Json.parse(jsonState)
      val offset = (state \ "offset").as[Long]
      val partition = (state \ "partition").as[Long]
      val ttopic = (state \ "topic").as[String]
      (partition.toInt, offset)
    }).toMap
  }

  def getKafkaState(topic: String): Map[Int, Long] = {

    val kParts = zk.getChildren(makePath(Seq(kafkaZkRoot, Some("brokers/topics"), Some(topic), Some("partitions"))))
    kParts.map({ kp =>
      val jsonState = new String(zk.get(makePath(Seq(kafkaZkRoot, Some("brokers/topics"), Some(topic), Some("partitions"), Some(kp), Some("state")))))
      val state = Json.parse(jsonState)
      val leader = (state \ "leader").as[Long]

      val idJson = new String(zk.get(makePath(Seq(kafkaZkRoot, Some("brokers/ids"), Some(leader.toString)))))
      val leaderState = Json.parse(idJson)
      val host = (leaderState \ "host").as[String]
      val port = (leaderState \ "port").as[Int]

      val ks = new SimpleConsumer(host, port, 1000000, 64*1024, "capillary")
      val topicAndPartition = TopicAndPartition(topic, kp.toInt)
      val requestInfo = Map[TopicAndPartition, PartitionOffsetRequestInfo](
          topicAndPartition -> new PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)
      )
      val request = new OffsetRequest(
        requestInfo = requestInfo, versionId = OffsetRequest.CurrentVersion, clientId = "capillary")
      val response = ks.getOffsetsBefore(request);
      if(response.hasError) {
        println("ERROR!")
      }
      val offset = response.partitionErrorAndOffsets.get(topicAndPartition).get.offsets(0)
      ks.close
      (kp.toInt, offset)
    }).toMap
  }
}