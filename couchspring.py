
import requests
import numpy
import socket
import bernhard
import ConfigParser
import os
try:
  import json
except ImportError:
  import simplejson as json


config = ConfigParser.RawConfigParser()   
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
configpath = os.path.join(__location__, 'couchspring.cfg')
config.read(configpath)

  
if 'global' in config.sections():
	couchbase_server = config.get('global', 'couchbase_server')
	cbuser = config.get('global', 'cbuser')
	cbpass = config.get('global', 'cbpass')
	sample_interval = config.get('global', 'sample_interval')
	stats = config.get('global', 'stats')
	mode = config.get('global', 'mode')
	ignore = config.get('global', 'ignore')
	
if 'riemann' in config.sections():
	riemann_server = config.get('riemann', 'riemann_server')
	riemann_port = config.get('riemann', 'riemann_port')
	riemann_enabled = config.get('riemann', 'enabled')
	
if 'influxdb' in config.sections():
	influx_server = config.get('influxdb', 'influx_server')
	influx_port = config.get('influxdb', 'influx_port')
	influx_enabled = config.get('influxdb', 'enabled')

if 'tags' in (config.options('includes')):
	tags = config.get('includes', 'tags')
if 'location' in (config.options('includes')):
	location = config.get('includes', 'location')
	




def sendRiemannEvent(**kwargs):
	if kwargs is not None:
		message = {}
		attribs = {}
		for key, value in kwargs.iteritems():
			if key in ['host','service','ttl','metric','state']:
				message[key] = value
			else:
				attribs[key] = value
		if any(attribs):
			message['attributes'] = attribs
		if tags is not None:
			message['tags'] = [tags]
		if mode == "debug":
			print (message)
		c.send(message)
		





nodeurl = 'http://'+couchbase_server+':8091/pools/nodes'
bucketsurl = 'http://'+couchbase_server+':8091/pools/default/buckets'




c = bernhard.Client(host=riemann_server, port=riemann_port, transport='UDPTransport')


r = requests.get(nodeurl, auth=(cbuser,cbpass))
json_data = json.loads(r.text)


for node in json_data['nodes']:
	nodecounter = 0
	nodename=(node['hostname']).replace(":8091","")
	
	swap_used_perc = (node['systemStats']['swap_used']*1.0) / node['systemStats']['swap_total'] * 100
	mem_used_perc = (node['memoryFree']*1.0) / node['memoryTotal'] * 100
	sendRiemannEvent( service= "%s/%s/%s" % ('node',nodename,'swap_used_percent'), host = couchbase_server, metric = swap_used_perc)
	sendRiemannEvent( service= "%s/%s/%s" % ('node',nodename,'memory_used_percent'), host = couchbase_server, metric = 100 - mem_used_perc)
	sendRiemannEvent( service= "%s/%s/%s" % ('node',nodename,'mcdMemoryReserved'), host = couchbase_server, metric = node['mcdMemoryReserved'])
	sendRiemannEvent( service= "%s/%s/%s" % ('node',nodename,'uptime'), host = couchbase_server, metric =  float(node['uptime']))

	if node['status'] == 'healthy':
		sendRiemannEvent( service= "%s/%s/%s" % ('node',nodename,'state'), host = couchbase_server, state = 'ok', metric=1)
	else:
		sendRiemannEvent( service= "%s/%s/%s" % ('node',nodename,'state'), host = couchbase_server, state = 'critical', metric=2)
	nodecounter  += 5
	
	for sstats in node['systemStats']:
		sendRiemannEvent( service= "%s/%s/%s" % ('node',nodename,sstats), host = couchbase_server, metric = node['systemStats'][sstats])
		nodecounter  += 1
	for istats in node['interestingStats']:
		sendRiemannEvent( service= "%s/%s/%s" % ('node',nodename,istats), host = couchbase_server, metric = node['interestingStats'][istats])
		nodecounter  += 1
	if mode in ["info","debug"]:
		print "metrics sent for node: " + nodename + " - " +  str(nodecounter	)
		
		
		
	
r = requests.get(bucketsurl, auth=(cbuser,cbpass))
json_data = json.loads(r.text)
buckets = []
[buckets.append(bucket['name']) for bucket in json_data] 
for bucket in buckets:
	bucketurl = 'http://'+couchbase_server+':8091/pools/default/buckets/'+bucket+'/stats'
	r = requests.get(bucketurl,auth=(cbuser,cbpass))
	json_data = json.loads(r.text)
	metrics = json_data['op']['samples']
	bucketcounter = 0
	for metric in metrics:
		if metric in stats and metric not in ignore:
			avg_value = sum(metrics[metric], 0.0) / len(metrics[metric])
			sendRiemannEvent( service= "%s/%s/%s" % ('node',bucket,metric), host = couchbase_server, metric = avg_value)
			bucketcounter += 1
	sendRiemannEvent( service= "%s/%s/%s" % ('node',bucket,"metrics_sent"), host = couchbase_server, metric = bucketcounter)
	if mode in ["info","debug"]:
		print "metrics sent for bucket: " + bucket + " - " +  str(bucketcounter	)

	