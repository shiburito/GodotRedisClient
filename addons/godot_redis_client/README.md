# Godot Redis Client

See the examples folder for full up-to-date implementation. You can run the example scene which will default to a local redis instance.

I found the need for a simple redis client when needing to persist state of backend software so I created this.
This was largly built off of reading [Redis serialization protocol specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)

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
