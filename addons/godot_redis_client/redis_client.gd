extends Node
class_name RedisClient

signal message_received(channel: String, message: String)
signal pattern_message_received(pattern: String, channel: String, message: String)
signal subscribed(channel: String, subscription_count: int)
signal unsubscribed(channel: String, subscription_count: int)
signal pattern_subscribed(pattern: String, subscription_count: int)
signal pattern_unsubscribed(pattern: String, subscription_count: int)

var connection : StreamPeerTCP
var host : String
var port : int
var client_timeout_seconds : int
var request_timeout_seconds : int
var _command_buffer : PackedByteArray = []
var _pubsub_buffer : PackedByteArray = []
var _socket_buffer : PackedByteArray = []
var _pubsub_polling : bool = false
var _router_running : bool = false
var run_mode : RUN_MODE

enum RUN_MODE{DEFAULT,PUBLISH,SUBSCRIBE}

func _init(host : String = "127.0.0.1", port : int = 6379, client_timeout_seconds : int = 5, request_timeout_seconds : int = 30) -> void:
	self.host = host
	self.port = port
	self.client_timeout_seconds = client_timeout_seconds
	self.request_timeout_seconds = request_timeout_seconds

func connect_to_redis(credentials : Dictionary = {}) -> bool:
	connection = StreamPeerTCP.new() as StreamPeerTCP
	_command_buffer = []
	_pubsub_buffer = []
	_socket_buffer = []

	var err = connection.connect_to_host(host, port)

	if err != OK:
		push_error("Error connecting to redis: %s" % (host + ":" + str(port)))
		return false

	var elapsed = 0.0
	while connection.get_status() == StreamPeerTCP.STATUS_CONNECTING:
		connection.poll()
		await Engine.get_main_loop().create_timer(0.1).timeout
		elapsed += 0.1
		if elapsed > client_timeout_seconds:
			push_error("Error connecting to redis %s connection timed out" % (host + ":" + str(port)))
			return false

	if not connected():
		push_error("Failed to establish connection to Redis")
		return false

	_start_message_router()

	var hello_command = ["HELLO", "3"]
	
	if credentials.has("user") || credentials.has("pass"): hello_command.append("AUTH")
	if credentials.has("user"): hello_command.append(credentials["user"])
	if credentials.has("pass"): hello_command.append(credentials["pass"])
	
	var resp = await _send_command_array(hello_command)

	if resp is Dictionary and resp.has("error"):
		if resp["error"].begins_with("WRONGPASS"):
			push_error("Invalid credentials specified for connection " + (host + ":" + str(port)))
			push_error(resp["error"])
			return false
		push_warning("HELLO 3 failed, falling back to RESP2: %s" % resp["error"])

	return connected()

func connected() -> bool:
	return connection.get_status() == StreamPeerTCP.STATUS_CONNECTED

func subscribe_mode() -> bool:
	return run_mode == RUN_MODE.SUBSCRIBE

func publish_mode() -> bool:
	return run_mode == RUN_MODE.PUBLISH

func default_mode() -> bool:
	return run_mode == RUN_MODE.DEFAULT

func setex(key: String, value: String, ttl: int) -> bool:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run setex against a client in publish/subscribe mode %s" % (key + " " + value + " " + str(ttl)))
		return false
	
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["SETEX", key, str(ttl), value])
	if response is Dictionary and response.has("error"):
		push_error("SETEX failed: %s" % response["error"])
		return false
	return response == "OK"

func set_value(key: String, value: String) -> bool:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run set_value against a client in publish/subscribe mode %s" % (key + " " + value))
		return false
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["SET", key, value])
	if response is Dictionary and response.has("error"):
		push_error("SET failed: %s" % response["error"])
		return false

	return response == "OK"

func get_value(key: String) -> String:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run get_value against a client in publish/subscribe mode %s" % key)
		return ""
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["GET", key])
	if response is Dictionary and response.has("error"):
		push_error("GET failed: %s" % response["error"])
		return ""
	if response == null:
		return ""
	return str(response)

func delete(key: String) -> bool:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run delete against a client in publish/subscribe mode %s" % key)
		return false
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["DEL", key])
	if response is Dictionary and response.has("error"):
		push_error("DEL failed: %s" % response["error"])
		return false
	return response == 1

func incr(key: String) -> int:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run incr against a client in publish/subscribe mode %s" % key)
		return 0
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["INCR", key])
	if response is Dictionary and response.has("error"):
		push_error("INCR failed: %s" % response["error"])
		return 0
	return response if response is int else 0

func decr(key: String) -> int:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run decr against a client in publish/subscribe mode %s" % key)
		return 0
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["DECR", key])
	if response is Dictionary and response.has("error"):
		push_error("DECR failed: %s" % response["error"])
		return 0
	return response if response is int else 0

func incrby(key: String, increment: int) -> int:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run incrby against a client in publish/subscribe mode %s" % (key + " " + str(increment)))
		return 0
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["INCRBY", key, str(increment)])
	if response is Dictionary and response.has("error"):
		push_error("INCRBY failed: %s" % response["error"])
		return 0
	return response if response is int else 0

func decrby(key: String, decrement: int) -> int:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run decrby against a client in publish/subscribe mode %s" % (key + " " + str(decrement)))
		return 0
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["DECRBY", key, str(decrement)])
	if response is Dictionary and response.has("error"):
		push_error("DECRBY failed: %s" % response["error"])
		return 0
	return response if response is int else 0

func mget(keys: Array) -> Array:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run mget against a client in publish/subscribe mode %s" % str(keys))
		return []
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var args = ["MGET"]
	args.append_array(keys)
	var response = await _send_command_array(args)
	if response is Dictionary and response.has("error"):
		push_error("MGET failed: %s" % response["error"])
		return []
	if response is Array:
		return response
	return []

func mset(key_values: Dictionary) -> bool:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run mset against a client in publish/subscribe mode %s" % str(key_values))
		return false
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var args = ["MSET"]
	for key in key_values:
		args.append(key)
		args.append(str(key_values[key]))
	var response = await _send_command_array(args)
	if response is Dictionary and response.has("error"):
		push_error("MSET failed: %s" % response["error"])
		return false
	return response == "OK"

func getex(key: String, ttl: int) -> String:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run getex against a client in publish/subscribe mode %s" % (key + " " + str(ttl)))
		return ""
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["GETEX", key, "EX", str(ttl)])
	if response is Dictionary and response.has("error"):
		push_error("GETEX failed: %s" % response["error"])
		return ""
	if response == null:
		return ""
	return str(response)

func getdel(key: String) -> String:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run getdel against a client in publish/subscribe mode %s" % key)
		return ""
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["GETDEL", key])
	if response is Dictionary and response.has("error"):
		push_error("GETDEL failed: %s" % response["error"])
		return ""
	if response == null:
		return ""
	return str(response)

func hset_resource(key : String, resource : Resource, prop_names : Array[String] = []) -> bool:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run hset against a client in publish/subscribe mode %s" % (resource.resource_name + " " + resource.resource_path))
		return false
	if resource == null:
		printerr("You must specify a non-null resource %s" % key)
		return false	
		
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var custom_props = {}
	for prop in resource.get_property_list():
		if prop.usage & PROPERTY_USAGE_SCRIPT_VARIABLE:
			if prop_names.is_empty() || prop_names.has(prop.name): custom_props[prop.name] = resource.get(prop.name)
	
	return await hset_multi(key, custom_props)
	

func hset(key: String, field: String, value: String) -> bool:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run hset against a client in publish/subscribe mode %s" % (key + " " + field + " " + value))
		return false
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["HSET", key, field, value])
	if response is Dictionary and response.has("error"):
		push_error("HSET failed: %s" % response["error"])
		return false

	return response == 1 or response == 0

func hset_multi(key: String, fields: Dictionary) -> int:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run hset_multi against a client in publish/subscribe mode %s" % (key + " " + str(fields)))
		return -1
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var args = ["HSET", key]
	for field in fields:
		args.append(field)
		args.append(str(fields[field]))

	var response = await _send_command_array(args)
	if response is Dictionary and response.has("error"):
		push_error("HSET failed: %s" % response["error"])
		return -1
	return response

func hgetall(key: String) -> Dictionary:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run hgetall against a client in publish/subscribe mode %s" % key)
		return {}
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["HGETALL", key])
	if response is Dictionary and response.has("error"):
		push_error("HGETALL failed: %s" % response["error"])
		return {}
	return response

func hincrby(key: String, field: String, increment: int) -> int:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run hincrby against a client in publish/subscribe mode %s" % (key + " " + field + " " + str(increment)))
		return 0
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["HINCRBY", key, field, str(increment)])
	if response is Dictionary and response.has("error"):
		push_error("HINCRBY failed: %s" % response["error"])
		return 0
	return response if response is int else 0

func hget(key: String, field: String) -> String:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run hget against a client in publish/subscribe mode %s" % (key + " " + field))
		return ""
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["HGET", key, field])
	if response is Dictionary and response.has("error"):
		push_error("HGET failed: %s" % response["error"])
		return ""
	if response == null:
		return ""
	return str(response)

func hdel(key: String, fields: Array) -> int:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run hdel against a client in publish/subscribe mode %s" % (key + " " + str(fields)))
		return -1
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var args = ["HDEL", key]
	args.append_array(fields)
	var response = await _send_command_array(args)
	if response is Dictionary and response.has("error"):
		push_error("HDEL failed: %s" % response["error"])
		return -1
	return response if response is int else 0

func hexists(key: String, field: String) -> bool:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run hexists against a client in publish/subscribe mode %s" % (key + " " + field))
		return false
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["HEXISTS", key, field])
	if response is Dictionary and response.has("error"):
		push_error("HEXISTS failed: %s" % response["error"])
		return false
	return response == 1

func hincrbyfloat(key: String, field: String, increment: float) -> float:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run hincrbyfloat against a client in publish/subscribe mode %s" % (key + " " + field + " " + str(increment)))
		return 0.0
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["HINCRBYFLOAT", key, field, str(increment)])
	if response is Dictionary and response.has("error"):
		push_error("HINCRBYFLOAT failed: %s" % response["error"])
		return 0.0
	if response is String:
		return float(response)
	return float(response) if response else 0.0

func hkeys(key: String) -> Array:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run hkeys against a client in publish/subscribe mode %s" % key)
		return []
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["HKEYS", key])
	if response is Dictionary and response.has("error"):
		push_error("HKEYS failed: %s" % response["error"])
		return []
	if response is Array:
		return response
	return []

func hvals(key: String) -> Array:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run hvals against a client in publish/subscribe mode %s" % key)
		return []
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["HVALS", key])
	if response is Dictionary and response.has("error"):
		push_error("HVALS failed: %s" % response["error"])
		return []
	if response is Array:
		return response
	return []

func hlen(key: String) -> int:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run hlen against a client in publish/subscribe mode %s" % key)
		return 0
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["HLEN", key])
	if response is Dictionary and response.has("error"):
		push_error("HLEN failed: %s" % response["error"])
		return 0
	return response if response is int else 0

func expire(key: String, ttl: int) -> bool:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run expire against a client in publish/subscribe mode %s" % (key + " " + str(ttl)))
		return false
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["EXPIRE", key, str(ttl)])
	if response is Dictionary and response.has("error"):
		push_error("EXPIRE failed: %s" % response["error"])
		return false
	return response == 1

func exists(keys: Array) -> int:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run exists against a client in publish/subscribe mode %s" % str(keys))
		return 0
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var args = ["EXISTS"]
	args.append_array(keys)
	var response = await _send_command_array(args)
	if response is Dictionary and response.has("error"):
		push_error("EXISTS failed: %s" % response["error"])
		return 0
	return response if response is int else 0

func ttl(key: String) -> int:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run ttl against a client in publish/subscribe mode %s" % key)
		return -2
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["TTL", key])
	if response is Dictionary and response.has("error"):
		push_error("TTL failed: %s" % response["error"])
		return -2
	return response if response is int else -2

func pttl(key: String) -> int:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run pttl against a client in publish/subscribe mode %s" % key)
		return -2
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["PTTL", key])
	if response is Dictionary and response.has("error"):
		push_error("PTTL failed: %s" % response["error"])
		return -2
	return response if response is int else -2

func persist(key: String) -> bool:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run persist against a client in publish/subscribe mode %s" % key)
		return false
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["PERSIST", key])
	if response is Dictionary and response.has("error"):
		push_error("PERSIST failed: %s" % response["error"])
		return false
	return response == 1

func type(key: String) -> String:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run type against a client in publish/subscribe mode %s" % key)
		return "none"
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["TYPE", key])
	if response is Dictionary and response.has("error"):
		push_error("TYPE failed: %s" % response["error"])
		return "none"
	return str(response)

func rename(key: String, new_key: String) -> bool:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run rename against a client in publish/subscribe mode %s" % (key + " " + new_key))
		return false
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["RENAME", key, new_key])
	if response is Dictionary and response.has("error"):
		push_error("RENAME failed: %s" % response["error"])
		return false
	return response == "OK"

func sadd(key: String, members: Array) -> int:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run sadd against a client in publish/subscribe mode %s" % (key + " " + str(members)))
		return -1
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var args = ["SADD", key]
	for member in members:
		args.append(str(member))

	var response = await _send_command_array(args)
	if response is Dictionary and response.has("error"):
		push_error("SADD failed: %s" % response["error"])
		return -1
	return response if response is int else 0

func srem(key: String, members: Array) -> int:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run srem against a client in publish/subscribe mode %s" % (key + " " + str(members)))
		return -1
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var args = ["SREM", key]
	for member in members:
		args.append(str(member))

	var response = await _send_command_array(args)
	if response is Dictionary and response.has("error"):
		push_error("SREM failed: %s" % response["error"])
		return -1
	return response if response is int else 0

func smembers(key: String) -> Array:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run smembers against a client in publish/subscribe mode %s" % key)
		return []
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["SMEMBERS", key])
	if response is Dictionary and response.has("error"):
		push_error("SMEMBERS failed: %s" % response["error"])
		return []
	if response is Array:
		return response
	return []

func sismember(key: String, member: String) -> bool:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run sismember against a client in publish/subscribe mode %s" % (key + " " + member))
		return false
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["SISMEMBER", key, member])
	if response is Dictionary and response.has("error"):
		push_error("SISMEMBER failed: %s" % response["error"])
		return false
	return response == 1

func scard(key: String) -> int:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run scard against a client in publish/subscribe mode %s" % key)
		return 0
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["SCARD", key])
	if response is Dictionary and response.has("error"):
		push_error("SCARD failed: %s" % response["error"])
		return 0
	return response if response is int else 0

func sinter(keys: Array) -> Array:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run sinter against a client in publish/subscribe mode %s" % str(keys))
		return []
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var args = ["SINTER"]
	args.append_array(keys)
	var response = await _send_command_array(args)
	if response is Dictionary and response.has("error"):
		push_error("SINTER failed: %s" % response["error"])
		return []
	if response is Array:
		return response
	return []

func sunion(keys: Array) -> Array:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run sunion against a client in publish/subscribe mode %s" % str(keys))
		return []
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var args = ["SUNION"]
	args.append_array(keys)
	var response = await _send_command_array(args)
	if response is Dictionary and response.has("error"):
		push_error("SUNION failed: %s" % response["error"])
		return []
	if response is Array:
		return response
	return []

func sdiff(keys: Array) -> Array:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run sdiff against a client in publish/subscribe mode %s" % str(keys))
		return []
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var args = ["SDIFF"]
	args.append_array(keys)
	var response = await _send_command_array(args)
	if response is Dictionary and response.has("error"):
		push_error("SDIFF failed: %s" % response["error"])
		return []
	if response is Array:
		return response
	return []

func spop(key: String, count: int = 1) -> Array:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run spop against a client in publish/subscribe mode %s" % (key + " " + str(count)))
		return []
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response
	if count == 1:
		response = await _send_command_array(["SPOP", key])
	else:
		response = await _send_command_array(["SPOP", key, str(count)])
	if response is Dictionary and response.has("error"):
		push_error("SPOP failed: %s" % response["error"])
		return []
	if response is Array:
		return response
	elif response == null:
		return []
	else:
		return [response]

func srandmember(key: String, count: int = 1) -> Array:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run srandmember against a client in publish/subscribe mode %s" % (key + " " + str(count)))
		return []
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response
	if count == 1:
		response = await _send_command_array(["SRANDMEMBER", key])
	else:
		response = await _send_command_array(["SRANDMEMBER", key, str(count)])
	if response is Dictionary and response.has("error"):
		push_error("SRANDMEMBER failed: %s" % response["error"])
		return []
	if response is Array:
		return response
	elif response == null:
		return []
	else:
		return [response]

func smove(source: String, destination: String, member: String) -> bool:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run smove against a client in publish/subscribe mode %s" % (source + " " + destination + " " + member))
		return false
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["SMOVE", source, destination, member])
	if response is Dictionary and response.has("error"):
		push_error("SMOVE failed: %s" % response["error"])
		return false
	return response == 1

func lpush(key: String, values: Array) -> int:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run lpush against a client in publish/subscribe mode %s" % (key + " " + str(values)))
		return -1
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var args = ["LPUSH", key]
	for value in values:
		args.append(str(value))
	var response = await _send_command_array(args)
	if response is Dictionary and response.has("error"):
		push_error("LPUSH failed: %s" % response["error"])
		return -1
	return response if response is int else 0

func rpush(key: String, values: Array) -> int:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run rpush against a client in publish/subscribe mode %s" % (key + " " + str(values)))
		return -1
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var args = ["RPUSH", key]
	for value in values:
		args.append(str(value))
	var response = await _send_command_array(args)
	if response is Dictionary and response.has("error"):
		push_error("RPUSH failed: %s" % response["error"])
		return -1
	return response if response is int else 0

func lpop(key: String, count: int = 1) -> Array:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run lpop against a client in publish/subscribe mode %s" % (key + " " + str(count)))
		return []
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response
	if count == 1:
		response = await _send_command_array(["LPOP", key])
	else:
		response = await _send_command_array(["LPOP", key, str(count)])
	if response is Dictionary and response.has("error"):
		push_error("LPOP failed: %s" % response["error"])
		return []
	if response is Array:
		return response
	elif response == null:
		return []
	else:
		return [str(response)]

func rpop(key: String, count: int = 1) -> Array:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run rpop against a client in publish/subscribe mode %s" % (key + " " + str(count)))
		return []
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response
	if count == 1:
		response = await _send_command_array(["RPOP", key])
	else:
		response = await _send_command_array(["RPOP", key, str(count)])
	if response is Dictionary and response.has("error"):
		push_error("RPOP failed: %s" % response["error"])
		return []
	if response is Array:
		return response
	elif response == null:
		return []
	else:
		return [str(response)]

func lrange(key: String, start: int, stop: int) -> Array:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run lrange against a client in publish/subscribe mode %s" % (key + " " + str(start) + " " + str(stop)))
		return []
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["LRANGE", key, str(start), str(stop)])
	if response is Dictionary and response.has("error"):
		push_error("LRANGE failed: %s" % response["error"])
		return []
	if response is Array:
		return response
	return []

func llen(key: String) -> int:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run llen against a client in publish/subscribe mode %s" % key)
		return 0
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["LLEN", key])
	if response is Dictionary and response.has("error"):
		push_error("LLEN failed: %s" % response["error"])
		return 0
	return response if response is int else 0

func lindex(key: String, index: int) -> String:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run lindex against a client in publish/subscribe mode %s" % (key + " " + str(index)))
		return ""
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["LINDEX", key, str(index)])
	if response is Dictionary and response.has("error"):
		push_error("LINDEX failed: %s" % response["error"])
		return ""
	if response == null:
		return ""
	return str(response)

func ltrim(key: String, start: int, stop: int) -> bool:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run ltrim against a client in publish/subscribe mode %s" % (key + " " + str(start) + " " + str(stop)))
		return false
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["LTRIM", key, str(start), str(stop)])
	if response is Dictionary and response.has("error"):
		push_error("LTRIM failed: %s" % response["error"])
		return false
	return response == "OK"

func blpop(keys: Array, timeout_seconds: int) -> Array:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run blpop against a client in publish/subscribe mode %s" % str(keys))
		return []
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var args = ["BLPOP"]
	args.append_array(keys)
	args.append(str(timeout_seconds))
	var response = await _send_command_array(args)
	if response is Dictionary and response.has("error"):
		push_error("BLPOP failed: %s" % response["error"])
		return []
	if response is Array:
		return response
	return []

func brpop(keys: Array, timeout_seconds: int) -> Array:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run brpop against a client in publish/subscribe mode %s" % str(keys))
		return []
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var args = ["BRPOP"]
	args.append_array(keys)
	args.append(str(timeout_seconds))
	var response = await _send_command_array(args)
	if response is Dictionary and response.has("error"):
		push_error("BRPOP failed: %s" % response["error"])
		return []
	if response is Array:
		return response
	return []

func zadd(key: String, score_members: Dictionary) -> int:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run zadd against a client in publish/subscribe mode %s" % (key + " " + str(score_members)))
		return -1
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var args = ["ZADD", key]
	for member in score_members:
		args.append(str(score_members[member]))
		args.append(str(member))
	var response = await _send_command_array(args)
	if response is Dictionary and response.has("error"):
		push_error("ZADD failed: %s" % response["error"])
		return -1
	return response if response is int else 0

func zrem(key: String, members: Array) -> int:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run zrem against a client in publish/subscribe mode %s" % (key + " " + str(members)))
		return -1
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var args = ["ZREM", key]
	args.append_array(members)
	var response = await _send_command_array(args)
	if response is Dictionary and response.has("error"):
		push_error("ZREM failed: %s" % response["error"])
		return -1
	return response if response is int else 0

func zrange(key: String, start: int, stop: int, with_scores: bool = false) -> Array:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run zrange against a client in publish/subscribe mode %s" % (key + " " + str(start) + " " + str(stop)))
		return []
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var args = ["ZRANGE", key, str(start), str(stop)]
	if with_scores:
		args.append("WITHSCORES")
	var response = await _send_command_array(args)
	if response is Dictionary and response.has("error"):
		push_error("ZRANGE failed: %s" % response["error"])
		return []
	if response is Array:
		return response
	return []

func zrangebyscore(key: String, min_score: String, max_score: String, with_scores: bool = false) -> Array:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run zrangebyscore against a client in publish/subscribe mode %s" % (key + " " + min_score + " " + max_score))
		return []
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var args = ["ZRANGEBYSCORE", key, min_score, max_score]
	if with_scores:
		args.append("WITHSCORES")
	var response = await _send_command_array(args)
	if response is Dictionary and response.has("error"):
		push_error("ZRANGEBYSCORE failed: %s" % response["error"])
		return []
	if response is Array:
		return response
	return []

func zincrby(key: String, increment: float, member: String) -> float:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run zincrby against a client in publish/subscribe mode %s" % (key + " " + str(increment) + " " + member))
		return 0.0
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["ZINCRBY", key, str(increment), member])
	if response is Dictionary and response.has("error"):
		push_error("ZINCRBY failed: %s" % response["error"])
		return 0.0
	if response is String:
		return float(response)
	return float(response) if response else 0.0

func zcard(key: String) -> int:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run zcard against a client in publish/subscribe mode %s" % key)
		return 0
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["ZCARD", key])
	if response is Dictionary and response.has("error"):
		push_error("ZCARD failed: %s" % response["error"])
		return 0
	return response if response is int else 0

func zscore(key: String, member: String) -> String:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run zscore against a client in publish/subscribe mode %s" % (key + " " + member))
		return ""
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["ZSCORE", key, member])
	if response is Dictionary and response.has("error"):
		push_error("ZSCORE failed: %s" % response["error"])
		return ""
	if response == null:
		return ""
	return str(response)

func zrank(key: String, member: String) -> int:
	if run_mode && run_mode != RUN_MODE.DEFAULT:
		printerr("You may not run zrank against a client in publish/subscribe mode %s" % (key + " " + member))
		return -1
	if !run_mode: run_mode = RUN_MODE.DEFAULT
	var response = await _send_command_array(["ZRANK", key, member])
	if response is Dictionary and response.has("error"):
		push_error("ZRANK failed: %s" % response["error"])
		return -1
	if response == null:
		return -1
	return response if response is int else -1

func publish(channel: String, message: String) -> int:
	if run_mode && run_mode != RUN_MODE.PUBLISH:
		printerr("You may not run publish against a client in default/subscribe mode %s" % (channel + " " + message))
		return -1
	
	if !run_mode: run_mode = RUN_MODE.PUBLISH
	var response = await _send_command_array(["PUBLISH", channel, message])
	if response is Dictionary and response.has("error"):
		push_error("PUBLISH failed: %s" % response["error"])
		return 0
	return response if response is int else 0

func subscribe(channels: Array) -> bool:
	if run_mode && run_mode != RUN_MODE.SUBSCRIBE:
		printerr("You may not run subscribe against a client in default/publish mode %s" % str(channels))
		return false
		
	if channels.is_empty():
		push_error("No channels provided for subscription")
		return false

	var args = ["SUBSCRIBE"]
	args.append_array(channels)

	var encoded = _encode_command(args)
	connection.put_data(encoded)

	if !run_mode: run_mode = RUN_MODE.SUBSCRIBE
	_start_pubsub_polling()

	return true

func unsubscribe(channels: Array = []) -> bool:
	if run_mode && run_mode != RUN_MODE.SUBSCRIBE:
		printerr("You may not run unsubscribe against a client in default/publish mode %s" % str(channels))
		return false

	var args = ["UNSUBSCRIBE"]
	if not channels.is_empty():
		args.append_array(channels)

	var encoded = _encode_command(args)
	connection.put_data(encoded)
	if !run_mode: run_mode = RUN_MODE.SUBSCRIBE
	
	return true

func psubscribe(patterns: Array) -> bool:
	if run_mode && run_mode != RUN_MODE.SUBSCRIBE:
		printerr("You may not run psubscribe against a client in default/publish mode %s" % str(patterns))
		return false
		
	if patterns.is_empty():
		push_error("No patterns provided for subscription")
		return false

	var args = ["PSUBSCRIBE"]
	args.append_array(patterns)

	var encoded = _encode_command(args)
	connection.put_data(encoded)

	if !run_mode: run_mode = RUN_MODE.SUBSCRIBE
	_start_pubsub_polling()

	return true

func punsubscribe(patterns: Array = []) -> bool:
	if run_mode && run_mode != RUN_MODE.SUBSCRIBE:
		printerr("You may not run punsubscribe against a client in default/publish mode %s" % str(patterns))
		return false

	var args = ["PUNSUBSCRIBE"]
	if not patterns.is_empty():
		args.append_array(patterns)

	var encoded = _encode_command(args)
	connection.put_data(encoded)
	if !run_mode: run_mode = RUN_MODE.SUBSCRIBE

	return true

func _start_pubsub_polling():
	if _pubsub_polling:
		return

	_pubsub_polling = true
	_poll_pubsub_messages()

func _poll_pubsub_messages():
	while _pubsub_polling and connected():
		while true:
			var result = _try_parse_resp_from_buffer(_pubsub_buffer)
			if result.has("incomplete"):
				break
			elif result.has("error"):
				push_error("Pub/Sub parse error: %s" % result["error"])
				break
			elif result.has("value"):
				_handle_pubsub_message(result["value"])

		await Engine.get_main_loop().create_timer(0.01).timeout

	_pubsub_polling = false

func _handle_pubsub_message(msg):
	if not (msg is Array) or msg.is_empty():
		return

	var msg_type = str(msg[0])

	match msg_type:
		"subscribe":
			if msg.size() >= 3:
				subscribed.emit(str(msg[1]), int(msg[2]))
		"unsubscribe":
			if msg.size() >= 3:
				unsubscribed.emit(str(msg[1]), int(msg[2]))
				if int(msg[2]) == 0:
					run_mode = RUN_MODE.DEFAULT
					_pubsub_polling = false
		"psubscribe":
			if msg.size() >= 3:
				pattern_subscribed.emit(str(msg[1]), int(msg[2]))
		"punsubscribe":
			if msg.size() >= 3:
				pattern_unsubscribed.emit(str(msg[1]), int(msg[2]))
				if int(msg[2]) == 0:
					run_mode = RUN_MODE.DEFAULT
					_pubsub_polling = false
		"message":
			if msg.size() >= 3:
				message_received.emit(str(msg[1]), str(msg[2]))
		"pmessage":
			if msg.size() >= 4:
				pattern_message_received.emit(str(msg[1]), str(msg[2]), str(msg[3]))

func _encode_command(args: Array) -> PackedByteArray:
	var result = PackedByteArray()

	var array_header = "*%d\r\n" % args.size()
	result.append_array(array_header.to_utf8_buffer())

	for arg in args:
		var arg_str = str(arg)
		var bulk_header = "$%d\r\n" % arg_str.length()
		result.append_array(bulk_header.to_utf8_buffer())
		result.append_array(arg_str.to_utf8_buffer())
		result.append_array("\r\n".to_utf8_buffer())

	return result

func _start_message_router():
	if _router_running:
		return

	_router_running = true
	_route_messages()

func _route_messages():
	while _router_running and connected():
		connection.poll()

		if connection.get_available_bytes() > 0:
			var data = connection.get_data(connection.get_available_bytes())
			if data[0] == OK:
				_socket_buffer.append_array(data[1])

		while _socket_buffer.size() > 0:
			var first_byte = char(_socket_buffer[0])

			if first_byte == ">": # Pub / Sub Oparations
				var result = _try_parse_resp_from_buffer(_socket_buffer)
				if result.has("incomplete"):
					break
				elif result.has("value") or result.has("error"):
					_pubsub_buffer.append_array(_encode_resp_value(result))
			else: # Regular Operations
				var result = _try_parse_resp_from_buffer(_socket_buffer)
				if result.has("incomplete"):
					break
				elif result.has("value") or result.has("error"):
					if _is_pubsub_message(result.get("value")):
						_pubsub_buffer.append_array(_encode_resp_value(result))
					else:
						_command_buffer.append_array(_encode_resp_value(result))

		await Engine.get_main_loop().create_timer(0.005).timeout

	_router_running = false

func _encode_resp_value(parsed_result: Dictionary) -> PackedByteArray:
	var result = PackedByteArray()

	if parsed_result.has("value"):
		var value = parsed_result["value"]
		result.append_array(_encode_resp_type(value))
	elif parsed_result.has("error"):
		var error_str = "-%s\r\n" % parsed_result["error"]
		result.append_array(error_str.to_utf8_buffer())

	return result

func _encode_resp_type(value) -> PackedByteArray:
	var result = PackedByteArray()

	if value == null:
		result.append_array("_\r\n".to_utf8_buffer())
	elif value is String:
		var bulk = "$%d\r\n%s\r\n" % [value.length(), value]
		result.append_array(bulk.to_utf8_buffer())
	elif value is int:
		var int_str = ":%d\r\n" % value
		result.append_array(int_str.to_utf8_buffer())
	elif value is float:
		var double_str = ",%s\r\n" % str(value)
		result.append_array(double_str.to_utf8_buffer())
	elif value is Array:
		var arr_header = "*%d\r\n" % value.size()
		result.append_array(arr_header.to_utf8_buffer())
		for item in value:
			result.append_array(_encode_resp_type(item))
	elif value is Dictionary:
		var map_header = "%%%d\r\n" % value.size()
		result.append_array(map_header.to_utf8_buffer())
		for key in value:
			result.append_array(_encode_resp_type(key))
			result.append_array(_encode_resp_type(value[key]))

	return result

func _send_command_array(args: Array):
	if not connected():
		push_error("You are not connected to redis, please connect to redis before making a request!")
		return null

	var encoded = _encode_command(args)
	connection.put_data(encoded)

	var response = await _read_resp_response()
	return response

func _read_resp_response():
	var start_time = Time.get_ticks_msec() / 1000.0

	while true:
		var elapsed = (Time.get_ticks_msec() / 1000.0) - start_time
		if elapsed > request_timeout_seconds:
			push_error("Timeout waiting for Redis response")
			return null

		var result = _try_parse_resp_from_buffer(_command_buffer)
		if result.has("value"):
			return result["value"]
		elif result.has("error"):
			return {"error": result["error"]}

		await Engine.get_main_loop().create_timer(0.01).timeout

func _try_parse_resp_from_buffer(buffer: PackedByteArray) -> Dictionary:
	if buffer.size() == 0:
		return {"incomplete": true}

	var first_byte = char(buffer[0])

	match first_byte:
		"+":
			return _parse_simple_string(buffer)
		"-":
			return _parse_error(buffer)
		":":
			return _parse_integer(buffer)
		"#":
			return _parse_boolean(buffer)
		",":
			return _parse_double(buffer)
		"$":
			return _parse_bulk_string(buffer)
		"*":
			return _parse_array(buffer)
		"~":
			return _parse_set(buffer)
		"%":
			return _parse_map(buffer)
		">":
			return _parse_push(buffer)
		"_":
			if buffer.size() >= 3:
				for i in range(3):
					buffer.remove_at(0)
				return {"value": null}
			return {"incomplete": true}
		_:
			push_error("Unknown RESP type: %s" % first_byte)
			return {"error": "Unknown RESP type"}

func _parse_simple_string(buffer: PackedByteArray) -> Dictionary:
	var line_end = _find_crlf_in_buffer(buffer)
	if line_end == -1:
		return {"incomplete": true}

	var line = buffer.slice(1, line_end).get_string_from_utf8()
	for i in range(line_end + 2):
		buffer.remove_at(0)
	return {"value": line}

func _parse_error(buffer: PackedByteArray) -> Dictionary:
	var line_end = _find_crlf_in_buffer(buffer)
	if line_end == -1:
		return {"incomplete": true}

	var error_msg = buffer.slice(1, line_end).get_string_from_utf8()
	for i in range(line_end + 2):
		buffer.remove_at(0)
	return {"error": error_msg}

func _parse_integer(buffer: PackedByteArray) -> Dictionary:
	var line_end = _find_crlf_in_buffer(buffer)
	if line_end == -1:
		return {"incomplete": true}

	var num_str = buffer.slice(1, line_end).get_string_from_utf8()
	for i in range(line_end + 2):
		buffer.remove_at(0)
	return {"value": int(num_str)}

func _parse_boolean(buffer: PackedByteArray) -> Dictionary:
	if buffer.size() < 4:  # #t\r\n or #f\r\n
		return {"incomplete": true}

	var bool_char = char(buffer[1])
	for i in range(4):
		buffer.remove_at(0)

	return {"value": bool_char == "t"}

func _parse_double(buffer: PackedByteArray) -> Dictionary:
	var line_end = _find_crlf_in_buffer(buffer)
	if line_end == -1:
		return {"incomplete": true}

	var double_str = buffer.slice(1, line_end).get_string_from_utf8()
	for i in range(line_end + 2):
		buffer.remove_at(0)

	return {"value": float(double_str)}

func _parse_bulk_string(buffer: PackedByteArray) -> Dictionary:
	var line_end = _find_crlf_in_buffer(buffer)
	if line_end == -1:
		return {"incomplete": true}

	var length_str = buffer.slice(1, line_end).get_string_from_utf8()
	var length = int(length_str)

	if length == -1:
		for i in range(line_end + 2):
			buffer.remove_at(0)
		return {"value": null}

	var data_start = line_end + 2
	var data_end = data_start + length

	if buffer.size() < data_end + 2:
		return {"incomplete": true}

	var data = buffer.slice(data_start, data_end).get_string_from_utf8()
	for i in range(data_end + 2):
		buffer.remove_at(0)
	return {"value": data}

func _parse_array(buffer: PackedByteArray) -> Dictionary:
	var line_end = _find_crlf_in_buffer(buffer)
	if line_end == -1:
		return {"incomplete": true}

	var count_str = buffer.slice(1, line_end).get_string_from_utf8()
	var count = int(count_str)

	if count == -1:
		for i in range(line_end + 2):
			buffer.remove_at(0)
		return {"value": null}

	for i in range(line_end + 2):
		buffer.remove_at(0)

	var array = []
	for i in range(count):
		var element = _try_parse_resp_from_buffer(buffer)
		if element.has("incomplete"):
			return {"incomplete": true}
		elif element.has("error"):
			return element
		else:
			array.append(element["value"])

	return {"value": array}

func _parse_map(buffer: PackedByteArray) -> Dictionary:
	var line_end = _find_crlf_in_buffer(buffer)
	if line_end == -1:
		return {"incomplete": true}

	var count_str = buffer.slice(1, line_end).get_string_from_utf8()
	var count = int(count_str)

	for i in range(line_end + 2):
		buffer.remove_at(0)

	var map = {}
	for i in range(count):
		var key_result = _try_parse_resp_from_buffer(buffer)
		if key_result.has("incomplete"):
			return {"incomplete": true}
		elif key_result.has("error"):
			return key_result

		var value_result = _try_parse_resp_from_buffer(buffer)
		if value_result.has("incomplete"):
			return {"incomplete": true}
		elif value_result.has("error"):
			return value_result

		map[str(key_result["value"])] = value_result["value"]

	return {"value": map}

func _parse_set(buffer: PackedByteArray) -> Dictionary:
	var line_end = _find_crlf_in_buffer(buffer)
	if line_end == -1:
		return {"incomplete": true}

	var count_str = buffer.slice(1, line_end).get_string_from_utf8()
	var count = int(count_str)

	if count == -1:
		for i in range(line_end + 2):
			buffer.remove_at(0)
			return {"value": null}

	for i in range(line_end + 2):
		buffer.remove_at(0)

	var set_array = []
	for i in range(count):
		var element = _try_parse_resp_from_buffer(buffer)
		if element.has("incomplete"):
			return {"incomplete": true}
		elif element.has("error"):
			return element
		else:
			set_array.append(element["value"])

	return {"value": set_array}
		
func _parse_push(buffer: PackedByteArray) -> Dictionary:
	var line_end = _find_crlf_in_buffer(buffer)
	if line_end == -1:
		return {"incomplete": true}

	var count_str = buffer.slice(1, line_end).get_string_from_utf8()
	var count = int(count_str)

	for i in range(line_end + 2):
		buffer.remove_at(0)

	var array = []
	for i in range(count):
		var element = _try_parse_resp_from_buffer(buffer)
		if element.has("incomplete"):
			return {"incomplete": true}
		elif element.has("error"):
			return element
		else:
			array.append(element["value"])

	return {"value": array}

func _find_crlf_in_buffer(buffer: PackedByteArray) -> int:
	for i in range(buffer.size() - 1):
		if buffer[i] == 13 and buffer[i + 1] == 10:
			return i
	return -1

func _is_pubsub_message(value) -> bool:
	if not (value is Array) or value.is_empty():
		return false

	var first_element = str(value[0])
	return first_element in ["subscribe", "unsubscribe", "psubscribe", "punsubscribe", "message", "pmessage"]
