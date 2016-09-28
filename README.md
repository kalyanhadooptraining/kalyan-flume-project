# kalyan-flume-source
An Apache Flume Source that read JSON data from Twitter.

kalyan-flume-source assumes the body of an event contains a valid UTF-8 encoded JSON object.

# Flume configuration properties

Here is an example excerpt from a Flume configuration file with a Twitter Source configured for a Flume agent. ``Twitter tweets`` will read continuously convert into ``json`` data.

```
agent.sources = twitter

agent.sources.twitter.type = com.orienit.kalyan.flume.source.KalyanTwitterSource
agent.sources.twitter.consumerKey = ************
agent.sources.twitter.consumerSecret = ************
agent.sources.twitter.accessToken = ************
agent.sources.twitter.accessTokenSecret = ************
agent.sources.twitter.keywords = hadoop, big data, analytics
agent.sources.twitter.filter = false
```

## Prerequisites

Update ``consumerKey, consumerSecret, accessToken, accessTokenSecret and keywords`` information using Twitter API


###Examples

```
agent.sources.twitter.filter = true

this will give filter json data
```

```
agent.sources.twitter.filter = false

this will give full json data
```



# kalyan-flume-sink
An Apache Flume sink that writes JSON to a MongoDB collection.

kalyan-flume-sink assumes the body of an event contains a valid UTF-8 encoded JSON object.

# Flume configuration properties

Here is an example excerpt from a Flume configuration file with a MongoDB sink configured for a Flume agent. Events will be written to the ``json`` collection in the ``flume`` database.

```
agent.sinks = mongo

agent.sinks.mongo.type = com.orienit.kalyan.flume.sink.KalyanMongoSink
agent.sinks.mongo.hostNames = localhost
agent.sinks.mongo.database = flume
agent.sinks.mongo.collection = json
agent.sinks.mongo.user = admin
agent.sinks.mongo.password = admin
```

## Hostnames

Hostnames is a comma-separated list of MongoDB server hostnames on the form ``hostname:port``. If the port number is omitted, the default port 27017 will be used.

###Examples

Three servers, the default port number 27017 will be used:
```
agent.sinks.mongo.hostNames = mongo1,mongo2,mongo3
```

Fully specified host names:
```
agent.sinks.mongo.hostNames = mongodb:27017,mongodb:27018,mongodb:27019
```
