Capillary is a small web application that displays the state and
deltas of Storm spouts with a Kafka >= 0.8 cluster.

# Overview

Capillary does the following:
* Takes a spoutroot and topic name from the URL
* Fetches information about the topic's partitions and offsets from the Storm spout state in Zookeeper
* Fetches information about the partition's leaders from Zookeeper
* Fetches information from Kafka about the latest offset from the partitions leaders

# Structure

Storm's Kafka spout stores it's committed state in a ZK structure like this:

`/$SPOUT_ROOT/$CONSUMER_ID/$PARTITION_ID`

Each `$PARTITION_ID` stores state as a JSON object that looks like:

```json
{
  "topology": {
    "id": "$SOME_UNIQUE_ID",
    "name": "$TOPOLOGY_NAME"
  },
  "offset": $CURRENT_OFFSET,
  "partition": $PARTITION_NUMBER,
  "broker": {
    "host": "$HOST_ADDRESS",
    "port": 9092
  },
  "topic": "$TOPIC_NAME"
}
```