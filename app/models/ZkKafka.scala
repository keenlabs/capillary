package models

import kafka.api.{OffsetFetchRequest,OffsetRequest,PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.cache.PathChildrenCache
import org.apache.curator.retry.ExponentialBackoffRetry
import play.api.libs.json._
import play.api.Play
import play.api.Play.current
import scala.collection.JavaConverters._

object ZkKafka {

  case class Delta(partition: Int, amount: Option[Long], current: Long, storm: Option[Long])
  case class Topology(name: String, spoutRoot: String, topic: String)

  def topoCompFn(t1: Topology, t2: Topology) = {
    (t1.name compareToIgnoreCase t2.name) < 0
  }

  val zookeepers = Play.configuration.getString("capillary.zookeepers").getOrElse("localhost:2181")
  val kafkaZkRoot = Play.configuration.getString("capillary.kafka.zkroot")
  val stormZkRoot = Play.configuration.getString("capillary.storm.zkroot")

  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  val zkClient = CuratorFrameworkFactory.newClient(zookeepers, retryPolicy);
  zkClient.start();

  def makePath(parts: Seq[Option[String]]): String = {
    parts.foldLeft("")({ (path, maybeP) => maybeP.map({ p => path + "/" + p }).getOrElse(path) })
  }

  def getTopologies: Seq[Topology] = {
    val tops = zkClient.getChildren.forPath(makePath(Seq(stormZkRoot))).asScala.map({ r =>
      getSpoutTopology(r)
    }).sortWith(topoCompFn)
    tops
  }

  def getSpoutTopology(root: String): Topology = {
    // Fetch the spout root
    val s = zkClient.getChildren.forPath(makePath(Seq(stormZkRoot, Some(root))))
    // Fetch the partitions so we can pick the first one
    val parts = zkClient.getChildren.forPath(makePath(Seq(stormZkRoot, Some(root), Some(s.get(0)))))
    // Use the first partition's data to build up info about the topology
    val jsonState = new String(zkClient.getData.forPath(makePath(Seq(stormZkRoot, Some(root), Some(s.get(0)), Some(parts.get(0))))))
    val state = Json.parse(jsonState)
    val topic = (state \ "topic").as[String]
    val name = (state \ "topology" \ "name").as[String]
    Topology(name = name, topic = topic, spoutRoot = root)
  }

  def getSpoutState(root: String, topic: String): Map[Int, Long] = {
    // There is basically nothing for error checking in here.
    val s = zkClient.getChildren.forPath(makePath(Seq(stormZkRoot, Some(root))))

    // We assume that there's only one child. This might break things
    val parts = zkClient.getChildren.forPath(makePath(Seq(stormZkRoot, Some(root), Some(s.get(0)))))

    val states = parts.asScala.map({ vp =>
      val jsonState = zkClient.getData.forPath(makePath(Seq(stormZkRoot, Some(root), Some(s.get(0)), Some(vp))))
      val state = Json.parse(jsonState)
      val offset = (state \ "offset").as[Long]
      val partition = (state \ "partition").as[Long]
      val ttopic = (state \ "topic").as[String]
      (partition.toInt, offset)
    }).toMap
    states
  }

  def getKafkaState(topic: String): Map[Int, Long] = {
    val kParts = zkClient.getChildren.forPath(makePath(Seq(kafkaZkRoot, Some("brokers/topics"), Some(topic), Some("partitions"))))
    val states = kParts.asScala.map({ kp =>
      val jsonState = zkClient.getData.forPath(makePath(Seq(kafkaZkRoot, Some("brokers/topics"), Some(topic), Some("partitions"), Some(kp), Some("state"))))
      val state = Json.parse(jsonState)
      val leader = (state \ "leader").as[Long]

      val idJson = zkClient.getData.forPath(makePath(Seq(kafkaZkRoot, Some("brokers/ids"), Some(leader.toString))))
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
    states
  }
}