import pymongo, argparse, commands
from pymongo import Connection
import time, sys
from datetime import datetime
from datetime import timedelta
from socket import socket

thisHost = commands.getoutput('hostname')

parser = argparse.ArgumentParser(description='Creates graphite metrics for a single mongodb instance from administation commands.')
parser.add_argument('-host', default=thisHost,
                   help='host name of mongodb to create metrics from.')
parser.add_argument('-prefix', default='DEV',
                   help='prefix for all metrics.')
parser.add_argument('-service', default='unspecified', required=True,
                   help='service name the metrics should appear under.')
parser.add_argument('-graphiteHost', required=True,
                   help='host name for graphite server.')
parser.add_argument('-graphitePort', required=True,
                   help='port garphite is listening on.')
args = parser.parse_args()

carbonHost = args.graphiteHost
carbonPort = int(args.graphitePort)

mongoHost = args.host.lower()
mongoPort = 27017
connection = Connection(mongoHost, mongoPort)

metricName = args.prefix+'.'+args.service+'.mongodb.'

def uploadToCarbon(metrics):
  now = int( time.time() )
  lines = []

  for name, value in metrics.iteritems() :
    if name.find('mongo') == -1 :
      name = mongoHost.split('.')[0]+'.'+name
    lines.append(metricName+name+' %s %d' % (value, now))

  message = '\n'.join(lines) + '\n' 

  sock = socket()
  try:
    sock.connect( (carbonHost,carbonPort) )
  except:
    print "Couldn't connect to %(server)s on port %(port)d, is carbon-agent.py running?" % { 'server': carbonHost, 'port': carbonPort}
    sys.exit(1)
  print message
  #sock.sendall(message)

def calculateLagTimes(replStatus, primaryDate):
  lags = dict()
  for hostState in replStatus['members'] :
    lag = primaryDate - hostState['optimeDate']
    hostName = hostState['name'].lower().split('.')[0]
    lags[hostName+".lag_seconds"] = '%.0f' % ((lag.microseconds + (lag.seconds + lag.days * 24 * 3600) * 10**6) / 10**6)
  return lags

def gatherReplicationMetrics():
  replicaMetrics = dict()
  replStatus = connection.admin.command("replSetGetStatus");

  for hostState in replStatus['members'] :
    if hostState['stateStr'] == 'PRIMARY' and hostState['name'].lower().startswith(mongoHost) :
      lags = calculateLagTimes(replStatus, hostState['optimeDate'])
      replicaMetrics.update(lags)
    if hostState['name'].lower().startswith(mongoHost) :
      thisHostsState = hostState

  replicaMetrics['state'] = thisHostsState['state']
  return replicaMetrics

def gatherServerStatusMetrics():
  serverMetrics = dict()
  serverStatus = connection.admin.command("serverStatus");
  
  serverMetrics['lock.ratio'] = '%.5f' % serverStatus['globalLock']['ratio']
  serverMetrics['lock.queue.total'] = serverStatus['globalLock']['currentQueue']['total']
  serverMetrics['lock.queue.readers'] = serverStatus['globalLock']['currentQueue']['readers']
  serverMetrics['lock.queue.writers'] = serverStatus['globalLock']['currentQueue']['writers']

  serverMetrics['connections.current'] = serverStatus['connections']['current']
  serverMetrics['connections.available'] = serverStatus['connections']['available']

  serverMetrics['indexes.missRatio'] = '%.5f' % serverStatus['indexCounters']['btree']['missRatio']
  serverMetrics['indexes.hits'] = serverStatus['indexCounters']['btree']['hits']
  serverMetrics['indexes.misses'] = serverStatus['indexCounters']['btree']['misses']

  serverMetrics['cursors.open'] = serverStatus['cursors']['totalOpen']
  serverMetrics['cursors.timedOut'] = serverStatus['cursors']['timedOut']

  serverMetrics['mem.residentMb'] = serverStatus['mem']['resident']
  serverMetrics['mem.virtualMb'] = serverStatus['mem']['virtual']
  serverMetrics['mem.mapped'] = serverStatus['mem']['mapped']
  serverMetrics['mem.pageFaults'] = serverStatus['extra_info']['page_faults']

  serverMetrics['asserts.warnings'] = serverStatus['asserts']['warning']
  serverMetrics['asserts.errors'] = serverStatus['asserts']['msg']

  return serverMetrics


metrics = dict()
metrics.update(gatherReplicationMetrics())
metrics.update(gatherServerStatusMetrics())

uploadToCarbon(metrics)
