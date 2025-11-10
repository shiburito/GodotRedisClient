# Godot Redis Client

I found the need for a simple redis client when needing to persist state of backend software so I created this.
This was largly built off of reading [Redis serialization protocol specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)

See the examples folder for full up-to-date implementation. Not every funciton is listed below! You can run the example scene which will default to a local redis instance. 
Note that the pub/sub examples use credentials (found below) so you'll need to create the test user if you wish to run those examples without them erroring out.

**NOTE** Be sure to use seperate connections for your regular redis calls (key gets, etc) and seperate for your pub / sub operations. The example in the examples folder shows this. I've built in error checking around this so if you mess up, it will show up in the error logs.

## Basic Usage
* `var redis : RedisClient = RedisClient.new() as RedisClient` - Initialize the client, can pass in host, port and timeouts
* `var connected = await redis.connect_to_redis()` - Starts the actual connection to the redis client
* `var auth_connected = await redis.connect_to_redis({"user": "test", "pass": "test123"})` - Connect with authentication info
* `await redis.setex("test_key", "Initial Value", 60)` - Set a value with an expiry(seconds)
* `await redis.set_value("test_key", "New Initial Value")` - Sets a value without an expiry
* `var value = await redis.get_value("test_key")` - Get a value
* `await redis.hset("user:100", "score", "500")` - Set a singular hash key/value
* `var user_data = await redis.hgetall("user:100")` - Fetch a hash value
* `await redis.hset_multi("user:100", {
		"name": "Jim",
		"score": "1000",
		"some_new_field": "WOOOO"
	})` - Set a key to a whole hash at once
* `var count = await redis.publish("updates", "Data was modified")
   print("Published message to %d subscribers" % count)` - Publish a message on a channel


## Signals
There are various signals available from the client. These are generally for Pub/Sub operations.

* signal message_received(channel: String, message: String)
* signal pattern_message_received(pattern: String, channel: String, message: String)
* signal subscribed(channel: String, subscription_count: int)
* signal unsubscribed(channel: String, subscription_count: int)
* signal pattern_subscribed(pattern: String, subscription_count: int)
* signal pattern_unsubscribed(pattern: String, subscription_count: int)

## Local Testing
If you'd like to quickly get going locally you can run the below to start redis on your host in docker
`docker run -d --name redis -p 6379:6379 -v redis-data:/data redis:latest redis-server --appendonly yes`

To create a 'test' user with the password 'test123' (the one the example uses) if you'd like to use auth you can run the below after redis has started. 
This gives the user very wide access so be sure to only do this for local testing purposes.
`docker exec -it redis redis-cli ACL SETUSER test ON '>test123' ~* '&*' +@all`

Monitor incoming commands on your redis instance
`docker exec -it redis redis-cli MONITOR`
