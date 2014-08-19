import urllib2
import json
import sys
from statsd import statsd

statsd.connect('localhost', 8125)

topology = sys.argv[1]
toporoot = sys.argv[2]
topic = sys.argv[3]

state = urllib2.urlopen(
    "http://localhost:9000/api/status?toporoot={}&topic={}".format(
        toporoot, topic
    )
).read()

data = json.loads(state)

amount = 0
for looplord in data:
    if looplord['amount'] is not None:
        statsd.histogram(
            'razor.kafkamon.topology.partition',
            amount,
            tags = [
                "topic:{}".format(sys),
                "topology:{}".format(topology),
                "partition:{}".format(looplord['partition'])
            ]
        )
        amount += looplord['amount']

statsd.histogram(
    'razor.kafkamon.total_delta',
    amount, tags = [ "topology:{}".format(topology) ]
)
