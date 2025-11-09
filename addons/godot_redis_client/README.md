# Godot Redis Client

See the examples folder for full up-to-date implementation. You can run the example scene which will default to a local redis instance.

I found the need for a simple redis client when needing to persist state of backend software so I created this.
This was largly built off of reading [Redis serialization protocol specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)

## Basic Usage
* `var redis : RedisClient = RedisClient.new() as RedisClient` - Initialize the client, can pass in host, port and timeouts
* `var connected = await redis.connect_to_redis()` - Starts the actual connection to the redis client
* `await redis.setex("test_key", "Initial Value", 60)` - Set a value with an expiry(seconds)
* `await redis.set_value("test_key", "New Initial Value")` - Sets a value without an expiry
* `var value = await redis.get_value("test_key")` - Get a value

## Signals
There are various signals available from the client. These are generally for Pub/Sub operations.

* signal message_received(channel: String, message: String)
* signal pattern_message_received(pattern: String, channel: String, message: String)
* signal subscribed(channel: String, subscription_count: int)
* signal unsubscribed(channel: String, subscription_count: int)
* signal pattern_subscribed(pattern: String, subscription_count: int)
* signal pattern_unsubscribed(pattern: String, subscription_count: int)
