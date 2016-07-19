package models

import java.io.{ByteArrayInputStream, InputStreamReader}
import java.nio.ByteBuffer
import java.util.zip.{GZIPInputStream, ZipException}

import com.google.common.io.CharStreams
import kafka.api.{FetchRequestBuilder, PartitionMetadata, TopicMetadataRequest}
import kafka.consumer.SimpleConsumer
import play.api.Play.current
import play.api.{Logger, Play}

/**
  * Contains code to read messages from kafka.
  */
object KafkaMessageReader {

  private val Kafkas = Play.configuration.getString("capillary.kafkas").getOrElse("127.0.0.1:9092")
  private val KafkaHosts = Kafkas.split(",")

  private val Timeout = 100000
  private val BufferSize = 10000000
  private val ClientId = "Capillary"

  def getMessageContent(topic: String, partition: Int, offset: Long): String = {
    findLeader(KafkaHosts, topic, partition) match {
      case Some(pm) => readMessage(pm, topic, offset)
      case None => s"Could not find any leader for partition: $partition"
    }
  }

  private def findLeader(kafkaHosts: Array[String], topic: String, partition: Int): Option[PartitionMetadata] = {
    val partitionMetadata: Option[PartitionMetadata] = kafkaHosts
      // split up connecting string so we can separate host from port
      .map(_.split(":"))
      // set up tuples like (host, port)
      .map { hostAndPort => (hostAndPort(0), hostAndPort(1).toInt) }
      // take a view of it so we don't evaluate the whole thing
      .view
      // build the consumer
      .map { t => new SimpleConsumer(t._1, t._2, Timeout, BufferSize, ClientId) }
      // get the possible partition metadata
      .flatMap(mapToPartitionMetadata(_, topic))
      // see if we can find one that matches
      .find(_.partitionId == partition)

    if (partitionMetadata.isEmpty) {
      Logger.error("No partition found")
    }
    partitionMetadata
  }

  private def mapToPartitionMetadata(simpleConsumer: SimpleConsumer, topic: String): Seq[PartitionMetadata] = {
    val topicMetadataRequest = new TopicMetadataRequest(Seq(topic), 0)
    val topicMetadataResponse = simpleConsumer.send(topicMetadataRequest)
    simpleConsumer.close()
    topicMetadataResponse.topicsMetadata.flatMap(_.partitionsMetadata)
  }

  private def readMessage(partitionMetadata: PartitionMetadata, topic: String, offset: Long): String = {
    val leadBroker = partitionMetadata.leader.get
    val simpleConsumer = new SimpleConsumer(leadBroker.host, leadBroker.port, Timeout, BufferSize, ClientId)
    val fetchRequest = new FetchRequestBuilder()
      .clientId(ClientId)
      .addFetch(topic, partitionMetadata.partitionId, offset, BufferSize)
      .build()
    val fetchResponse = simpleConsumer.fetch(fetchRequest)
    simpleConsumer.close()
    fetchResponse match {
      case fr if fr.hasError =>
        val errorMessage = s"Error while fetching from topic: $topic and offset: $offset. " +
          s"Error code: ${fetchResponse.errorCode(topic, partitionMetadata.partitionId)}"
        Logger.error(errorMessage)
        errorMessage
      case fr =>
        val bytes: Option[Array[Byte]] = fetchResponse
          .messageSet(topic, partitionMetadata.partitionId)
          .headOption
          .map(_.message.payload)
          .map(bufferToArray)
        bytes match {
          case Some(b) => readBytesAsGzippedString(b).getOrElse(readBytesAsString(b))
          case None => ""
        }
    }
  }

  private def bufferToArray(byteBuffer: ByteBuffer): Array[Byte] = {
    val bytes = new Array[Byte](byteBuffer.limit())
    byteBuffer.get(bytes)
    bytes
  }

  private def readBytesAsString(bytes: Array[Byte]) = {
    new String(bytes, "UTF-8")
  }

  private def readBytesAsGzippedString(bytes: Array[Byte]): Option[String] = {
    try {
      Some(CharStreams.toString(new InputStreamReader(new GZIPInputStream(new ByteArrayInputStream(bytes)))))
    } catch {
      case e: ZipException => None
    }
  }

}
