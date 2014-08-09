import urllib2
import json
import sys
from statsd import statsd

statsd.connect('localhost', 8125)

topology = sys.argv[1]

state = urllib2.urlopen(
    "http://localhost:9000/api/status?toporoot={}&topic={}".format(
        sys.argv[2], sys.argv[3]
    )
).read()

data = json.loads(state)

amount = 0
for looplord in data:
    if looplord['amount'] != None:
        amount += looplord['amount']

statsd.histogram(
    'razor.kafkamon.total_delta',
    amount, tags = [ "topology:{}".format(topology) ]
)