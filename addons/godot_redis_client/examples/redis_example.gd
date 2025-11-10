extends Node

var redis : RedisClient
var publish_client : RedisClient
var subscribe_client : RedisClient

func _ready() -> void:
	await _test_redis()

func _assert_equal(actual, expected, test_name: String) -> void:
	if actual == expected:
		print("✓ %s" % test_name)
	else:
		print("✗ %s - Expected: %s, Got: %s" % [test_name, expected, actual])

func _assert_true(condition: bool, test_name: String) -> void:
	if condition:
		print("✓ %s" % test_name)
	else:
		print("✗ %s - Expected true, got false" % test_name)

func _assert_contains(array: Array, item, test_name: String) -> void:
	if array.has(item):
		print("✓ %s" % test_name)
	else:
		print("✗ %s - Array %s does not contain %s" % [test_name, array, item])

func _test_redis() -> void:
	redis = RedisClient.new() as RedisClient
	publish_client = RedisClient.new() as RedisClient
	subscribe_client = RedisClient.new() as RedisClient
	
	var connected = await redis.connect_to_redis()

	var publish_connected = await publish_client.connect_to_redis({"user": "test", "pass": "test123"})
	var subscribe_connected = await subscribe_client.connect_to_redis({"user": "test", "pass": "test123"})
	
	subscribe_client.subscribed.connect(_on_subscribed)
	subscribe_client.message_received.connect(_on_message_received)
	
	_assert_true(connected, "Connect to redis without auth")
	_assert_true(publish_connected, "Connect to redis with auth (publish)")
	_assert_true(subscribe_connected, "Connect to redis with auth (subscribe)")

	if not connected:
		return

	# Clean up any leftover data from previous runs first
	await redis.delete("user:100")
	await redis.delete("test_key")
	await redis.delete("active_users")
	await redis.delete("player_count")
	await redis.delete("key1")
	await redis.delete("key2")
	await redis.delete("key3")
	await redis.delete("test_ttl_key")
	await redis.delete("new_key")
	await redis.delete("old_key")
	await redis.delete("zone_a")
	await redis.delete("zone_b")
	await redis.delete("task_queue")
	await redis.delete("leaderboard")

	await redis.hset_resource("testing", load("res://addons/godot_redis_client/examples/resource/test.tres"))

	await redis.setex("test_key", "Initial Value", 60)
	var value = await redis.get_value("test_key")
	_assert_equal(value, "Initial Value", "SETEX and GET string value")

	await redis.set_value("test_key", "New Initial Value")
	value = await redis.get_value("test_key")
	_assert_equal(value, "New Initial Value", "SET and GET updated value")
	
	await redis.hset("user:100", "name", "Bob")
	await redis.hset("user:100", "score", "500")
	var user_data = await redis.hgetall("user:100")
	_assert_equal(user_data.get("name"), "Bob", "HSET and HGETALL - name field")
	_assert_equal(user_data.get("score"), "500", "HSET and HGETALL - score field")

	await redis.hset_multi("user:100", {
		"name": "Jim",
		"score": "1000",
		"some_new_field": "WOOOO"
	})
	user_data = await redis.hgetall("user:100")
	_assert_equal(user_data.get("name"), "Jim", "HSET_MULTI - updated name")
	_assert_equal(user_data.get("score"), "1000", "HSET_MULTI - updated score")
	_assert_equal(user_data.get("some_new_field"), "WOOOO", "HSET_MULTI - new field")

	var new_score = await redis.hincrby("user:100", "score", 50)
	_assert_equal(new_score, 1050, "HINCRBY - increment by 50")
	new_score = await redis.hincrby("user:100", "score", -20)
	_assert_equal(new_score, 1030, "HINCRBY - decrement by 20")

	var added = await redis.sadd("active_users", ["user1", "user2", "user3"])
	_assert_equal(added, 3, "SADD - add 3 new members")

	added = await redis.sadd("active_users", ["user3", "user4", "user5"])
	_assert_equal(added, 2, "SADD - add 2 new members (1 duplicate ignored)")

	var members = await redis.smembers("active_users")
	_assert_equal(members.size(), 5, "SMEMBERS - all 5 members present")
	_assert_contains(members, "user1", "SMEMBERS - contains user1")
	_assert_contains(members, "user5", "SMEMBERS - contains user5")

	var is_member = await redis.sismember("active_users", "user2")
	_assert_true(is_member, "SISMEMBER - user2 exists in set")
	is_member = await redis.sismember("active_users", "user99")
	_assert_true(!is_member, "SISMEMBER - user99 does not exist in set")

	var removed = await redis.srem("active_users", ["user1", "user3"])
	_assert_equal(removed, 2, "SREM - removed 2 members")

	members = await redis.smembers("active_users")
	_assert_equal(members.size(), 3, "SMEMBERS - 3 members remain after removal")

	await redis.set_value("player_count", "100")
	var string_count = await redis.incr("player_count")
	_assert_equal(string_count, 101, "INCR - increment to 101")
	string_count = await redis.decr("player_count")
	_assert_equal(string_count, 100, "DECR - decrement to 100")
	string_count = await redis.incrby("player_count", 25)
	_assert_equal(string_count, 125, "INCRBY - increment by 25 to 125")
	string_count = await redis.decrby("player_count", 10)
	_assert_equal(string_count, 115, "DECRBY - decrement by 10 to 115")

	await redis.mset({"key1": "value1", "key2": "value2", "key3": "value3"})
	var values = await redis.mget(["key1", "key2", "key3"])
	_assert_equal(values[0], "value1", "MSET/MGET - key1")
	_assert_equal(values[1], "value2", "MSET/MGET - key2")
	_assert_equal(values[2], "value3", "MSET/MGET - key3")

	await redis.set_value("temp_key", "temporary_value")
	var temp_val = await redis.getex("temp_key", 5)
	_assert_equal(temp_val, "temporary_value", "GETEX - get value with expiry update")
	var deleted_val = await redis.getdel("temp_key")
	_assert_equal(deleted_val, "temporary_value", "GETDEL - get and delete atomically")

	var field_val = await redis.hget("user:100", "name")
	_assert_equal(field_val, "Jim", "HGET - get single field value")

	var field_exists = await redis.hexists("user:100", "name")
	_assert_true(field_exists, "HEXISTS - field 'name' exists")

	await redis.hset("user:100", "balance", "100.50")
	var new_balance = await redis.hincrbyfloat("user:100", "balance", 25.75)
	_assert_equal(new_balance, 126.25, "HINCRBYFLOAT - increment float value")

	var hash_keys = await redis.hkeys("user:100")
	_assert_contains(hash_keys, "name", "HKEYS - contains 'name'")
	_assert_contains(hash_keys, "score", "HKEYS - contains 'score'")
	var hash_vals = await redis.hvals("user:100")
	_assert_true(hash_vals.size() >= 3, "HVALS - has at least 3 values")
	var hash_len = await redis.hlen("user:100")
	_assert_equal(hash_len, 4, "HLEN - has 4 fields")

	var deleted_fields = await redis.hdel("user:100", ["some_new_field"])
	_assert_equal(deleted_fields, 1, "HDEL - deleted 1 field")

	var set_size = await redis.scard("active_users")
	_assert_equal(set_size, 3, "SCARD - set has 3 members")

	await redis.sadd("zone_a", ["server1", "server2", "server3"])
	await redis.sadd("zone_b", ["server2", "server3", "server4"])

	var intersection = await redis.sinter(["zone_a", "zone_b"])
	_assert_equal(intersection.size(), 2, "SINTER - 2 servers in both zones")
	_assert_contains(intersection, "server2", "SINTER - contains server2")
	var union = await redis.sunion(["zone_a", "zone_b"])
	_assert_equal(union.size(), 4, "SUNION - 4 unique servers total")
	var diff = await redis.sdiff(["zone_a", "zone_b"])
	_assert_equal(diff.size(), 1, "SDIFF - 1 server only in zone_a")
	_assert_contains(diff, "server1", "SDIFF - contains server1")

	var random_servers = await redis.srandmember("zone_a", 2)
	_assert_equal(random_servers.size(), 2, "SRANDMEMBER - returned 2 random members")

	var moved = await redis.smove("zone_a", "zone_b", "server1")
	_assert_true(moved, "SMOVE - moved server1 to zone_b")

	var popped = await redis.spop("zone_a", 1)
	_assert_equal(popped.size(), 1, "SPOP - popped 1 member from set")

	var queue_len = await redis.rpush("task_queue", ["task1", "task2", "task3"])
	_assert_equal(queue_len, 3, "RPUSH - queue has 3 items")
	queue_len = await redis.lpush("task_queue", ["urgent_task"])
	_assert_equal(queue_len, 4, "LPUSH - queue has 4 items")

	var task_list = await redis.lrange("task_queue", 0, -1)
	_assert_equal(task_list.size(), 4, "LRANGE - retrieved all 4 tasks")
	_assert_equal(task_list[0], "urgent_task", "LRANGE - first task is urgent_task")

	var list_len = await redis.llen("task_queue")
	_assert_equal(list_len, 4, "LLEN - list has 4 items")

	var task_at_index = await redis.lindex("task_queue", 0)
	_assert_equal(task_at_index, "urgent_task", "LINDEX - task at index 0")

	var next_task = await redis.lpop("task_queue")
	_assert_equal(next_task[0], "urgent_task", "LPOP - popped urgent_task from left")
	var last_task = await redis.rpop("task_queue")
	_assert_equal(last_task[0], "task3", "RPOP - popped task3 from right")

	await redis.ltrim("task_queue", 0, 1)
	var trimmed_list = await redis.lrange("task_queue", 0, -1)
	_assert_equal(trimmed_list.size(), 2, "LTRIM - trimmed to 2 elements")

	var added_scores = await redis.zadd("leaderboard", {
		"player1": 1000,
		"player2": 1500,
		"player3": 800,
		"player4": 2000
	})
	_assert_equal(added_scores, 4, "ZADD - added 4 players to leaderboard")

	var player_score = await redis.zscore("leaderboard", "player2")
	_assert_equal(player_score, "1500.0", "ZSCORE - player2 score is 1500")

	var player_rank = await redis.zrank("leaderboard", "player2")
	_assert_equal(player_rank, 2, "ZRANK - player2 rank is 2 (0-based)")

	var newest_score = await redis.zincrby("leaderboard", 250.0, "player1")
	_assert_equal(newest_score, 1250.0, "ZINCRBY - player1 score incremented to 1250")

	var top_players = await redis.zrange("leaderboard", 0, 2, false)
	_assert_equal(top_players.size(), 3, "ZRANGE - returned 3 players")
	_assert_equal(top_players[0], "player3", "ZRANGE - lowest score is player3")

	var top_with_scores = await redis.zrange("leaderboard", 0, 2, true)
	_assert_true(top_with_scores.size() > 0, "ZRANGE with scores - returned results")

	var high_scorers = await redis.zrangebyscore("leaderboard", "1000", "2000", false)
	_assert_true(high_scorers.size() >= 2, "ZRANGEBYSCORE - found players in score range")

	var leaderboard_size = await redis.zcard("leaderboard")
	_assert_equal(leaderboard_size, 4, "ZCARD - leaderboard has 4 players")

	var removed_players = await redis.zrem("leaderboard", ["player3"])
	_assert_equal(removed_players, 1, "ZREM - removed 1 player")

	var key_exists = await redis.exists(["user:100", "player_count"])
	_assert_equal(key_exists, 2, "EXISTS - 2 keys exist")

	var key_type = await redis.type("user:100")
	_assert_equal(key_type, "hash", "TYPE - user:100 is a hash")

	await redis.set_value("test_ttl_key", "will_expire")
	await redis.expire("test_ttl_key", 60)
	var remaining_ttl = await redis.ttl("test_ttl_key")
	_assert_true(remaining_ttl > 0 && remaining_ttl <= 60, "TTL - expiry set correctly")

	var remaining_pttl = await redis.pttl("test_ttl_key")
	_assert_true(remaining_pttl > 0, "PTTL - returns milliseconds")

	var persisted = await redis.persist("test_ttl_key")
	_assert_true(persisted, "PERSIST - removed expiry")

	await redis.set_value("old_key", "value")
	var renamed = await redis.rename("old_key", "new_key")
	_assert_true(renamed, "RENAME - renamed key successfully")

	subscribe_client.subscribe(["updates"])
	await get_tree().create_timer(0.5).timeout

	var sub_count = await publish_client.publish("updates", "Data was modified")
	_assert_equal(sub_count, 1, "PUBLISH - message sent to 1 subscriber")

	await get_tree().create_timer(0.3).timeout

	sub_count = await publish_client.publish("updates", "More updates!")
	_assert_equal(sub_count, 1, "PUBLISH - second message sent to 1 subscriber")

	value = await redis.get_value("test_key")
	_assert_equal(value, "New Initial Value", "GET - final value check")

	await get_tree().create_timer(0.5).timeout

	# Unsubscribe from pub/sub operations, can pass in channels array
	subscribe_client.unsubscribe()
	await get_tree().create_timer(0.5).timeout

	print("\n=== Testing Error Handling (Expected Errors Below) ===")
	subscribe_client.publish("updates", "TEST")
	publish_client.expire("test", 1)

	print("\n=== All Tests Complete! ===")

	# Clean up all test data
	print("Cleaning up test data...")
	await redis.delete("user:100")
	await redis.delete("test_key")
	await redis.delete("active_users")
	await redis.delete("player_count")
	await redis.delete("key1")
	await redis.delete("key2")
	await redis.delete("key3")
	await redis.delete("test_ttl_key")
	await redis.delete("new_key")
	await redis.delete("zone_a")
	await redis.delete("zone_b")
	await redis.delete("task_queue")
	await redis.delete("leaderboard")
	print("Cleanup complete!")

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
