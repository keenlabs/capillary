import urllib2
import json

# WIP, ignore this

state = urllib2.urlopen("http://localhost:9000/api/status?toporoot=kendra-write_event-staging&topic=migration-staging").read()

data = json.loads(state)

for looplord in data:
    print looplord['amount']
