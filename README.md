# MongoDB Graphite Metrics
Small Python script that pointed at mongodb instance converts the server status information into metrics to be consumed and displayed by [Graphite][g_link]

[g_link]: http://graphite.wikidot.com/

Includes most of the db.serverStatus() information and also calculates the replication lag from the Primary node in a replica set.