import urllib2
import json
import sys
import time
import os
from statsd import statsd
import sys

statsd.connect('localhost', 8125)

def report_stats(host, topology, toporoot, topic):
        state = urllib2.urlopen(
            "http://{}/api/status?toporoot={}&topic={}".format(
                host, toporoot, topic
            )
        ).read()

        data = json.loads(state)

        amount = 0
        for looplord in data:
            if looplord['amount'] is not None:
                statsd.gauge(
                    'razor.kafkamon.topology.partition',
                    looplord['amount'],
                    tags = [
                        "topic:{}".format(topic),
                        "topology:{}".format(topology),
                        "partition:{}".format(looplord['partition'])
                    ]
                )
                amount += looplord['amount']

        print "Got {} for {}".format(amount, topology)

        statsd.gauge(
            'razor.kafkamon.total_delta',
            amount, tags = [
                "topic:{}".format(topic),
                "topology:{}".format(topology)
            ]
        )

host = sys.argv[1]
print "pulling stats from capillary at: {}".format(host)

reports = json.loads(open(sys.argv[2]).read())
print "Reporting on {}".format(json.dumps(reports, indent=4))

for topo, root, topic in reports:
  print "querying stats for topo:{} root:{} topic:{}".format(topo, root, topic)
  try:
    report_stats(host, topo, root, topic)
  except Exception as e:
    print "Failed to report metrics for {}/{}/{} because: {}".format(topo, root, topic, str(e))

sys.stdout.flush()
