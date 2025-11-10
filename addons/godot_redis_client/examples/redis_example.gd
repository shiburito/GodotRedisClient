extends Node

var redis : RedisClient
var publish_client : RedisClient
var subscribe_client : RedisClient

func _ready() -> void:
	await _test_redis()

func _test_redis() -> void:
	redis = RedisClient.new() as RedisClient
	publish_client = RedisClient.new() as RedisClient
	subscribe_client = RedisClient.new() as RedisClient
	
	var connected = await redis.connect_to_redis()
	# Connecting with a user/pass 
	var publish_connected = await publish_client.connect_to_redis({"user": "test", "pass": "test123"})
	var subscribe_connected = await subscribe_client.connect_to_redis({"user": "test", "pass": "test123"})
	
	subscribe_client.subscribed.connect(_on_subscribed)
	subscribe_client.message_received.connect(_on_message_received)
	
	print("Connected to redis: %s" % connected)
	print("Connected to publish redis with auth: %s" % publish_connected)
	print("Connected to subscribe redis with auth: %s" % subscribe_connected)
	
	if not connected:
		return

	# Set a key with expiry
	await redis.setex("test_key", "Initial Value", 60)
	var value = await redis.get_value("test_key")
	print("Stored value: %s" % value)
	# Set a key without expiry
	await redis.set_value("test_key", "New Initial Value")
	value = await redis.get_value("test_key")
	print("New Stored value: %s" % value)
	
	# Individual dictionary values sent
	await redis.hset("user:100", "name", "Bob")
	await redis.hset("user:100", "score", "500")
	var user_data = await redis.hgetall("user:100")
	print("User data: %s" % user_data)
	
	# Full Dictionary send
	await redis.hset_multi("user:100", {
		"name": "Jim",
		"score": "1000",
		"some_new_field": "WOOOO"
	})
	user_data = await redis.hgetall("user:100")
	print("Updated User data: %s" % user_data)

	# Subscribe to a channel
	subscribe_client.subscribe(["updates"])
	await get_tree().create_timer(0.5).timeout

	# Publish a message on a channel
	var count = await publish_client.publish("updates", "Data was modified")
	print("Published message to %d subscribers" % count)

	await get_tree().create_timer(0.3).timeout

	# Publish another message
	count = await publish_client.publish("updates", "More updates!")
	print("Published another message to %d subscribers" % count)
	
	value = await redis.get_value("test_key")
	print("Final value: %s" % value)

	await get_tree().create_timer(0.5).timeout

	print("Outputting expected errors")
	subscribe_client.publish("updates", "TEST")
	redis.subscribe(["test"])
	publish_client.expire("test", 1)
	
	# Unsubscribe from pub/sub operations, can pass in channels array
	subscribe_client.unsubscribe()
	await get_tree().create_timer(0.5).timeout
	
	# Delete a key
	await redis.delete("test_key")
	print("Deleted test key")
	print("Test complete!")
	
	await get_tree().create_timer(1.0).timeout
	get_tree().quit(0)

# Examples of signal processing messages, make sure to connect to the signals!
func _on_message_received(channel: String, message: String):
	print("[PUB/SUB MESSAGE] %s: %s" % [channel, message])

func _on_subscribed(channel: String, subscription_count: int):
	print("[SUBSCRIBED] %s (total: %d)" % [channel, subscription_count])

func _exit_tree():
	if subscribe_client and subscribe_client.run_mode == RedisClient.RUN_MODE.SUBSCRIBE:
		subscribe_client.unsubscribe()
