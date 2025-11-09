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
var _is_pubsub_mode : bool = false
var _pubsub_polling : bool = false
var _router_running : bool = false

const NOT_IN_PUB_SUB = "Not in pub/sub mode, nothing to unsubscribe"

func _init(host : String = "127.0.0.1", port : int = 6379, client_timeout_seconds : int = 5, request_timeout_seconds : int = 5) -> void:
	self.host = host
	self.port = port
	self.client_timeout_seconds = client_timeout_seconds
	self.request_timeout_seconds = request_timeout_seconds

func connect_to_redis() -> bool:
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

	var resp = await _send_command_array(["HELLO", "3"])

	if resp is Dictionary and resp.has("error"):
		push_warning("HELLO 3 failed, falling back to RESP2: %s" % resp["error"])

	return connected()

func connected() -> bool:
	return connection.get_status() == StreamPeerTCP.STATUS_CONNECTED

func setex(key: String, value: String, ttl: int) -> bool:
	var response = await _send_command_array(["SETEX", key, str(ttl), value])
	if response is Dictionary and response.has("error"):
		push_error("SETEX failed: %s" % response["error"])
		return false
	return response == "OK"

func set_value(key: String, value: String) -> bool:
	var response = await _send_command_array(["SET", key, value])
	if response is Dictionary and response.has("error"):
		push_error("SET failed: %s" % response["error"])
		return false
	print("RESP: " + response)
	return response == "OK"

func get_value(key: String) -> String:
	var response = await _send_command_array(["GET", key])
	if response is Dictionary and response.has("error"):
		push_error("GET failed: %s" % response["error"])
		return ""
	if response == null:
		return ""
	return str(response)

func delete(key: String) -> bool:
	var response = await _send_command_array(["DEL", key])
	if response is Dictionary and response.has("error"):
		push_error("DEL failed: %s" % response["error"])
		return false
	return response == 1

func hset(key: String, field: String, value: String) -> bool:
	var response = await _send_command_array(["HSET", key, field, value])
	if response is Dictionary and response.has("error"):
		push_error("HSET failed: %s" % response["error"])
		return false
	return response == 1 or response == 0

func hset_multi(key: String, fields: Dictionary) -> int:
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
	var response = await _send_command_array(["HGETALL", key])
	if response is Dictionary and response.has("error"):
		push_error("HGETALL failed: %s" % response["error"])
		return {}
	return response

func expire(key: String, ttl: int) -> bool:
	var response = await _send_command_array(["EXPIRE", key, str(ttl)])
	if response is Dictionary and response.has("error"):
		push_error("EXPIRE failed: %s" % response["error"])
		return false
	return response == 1

func publish(channel: String, message: String) -> int:
	var response = await _send_command_array(["PUBLISH", channel, message])
	if response is Dictionary and response.has("error"):
		push_error("PUBLISH failed: %s" % response["error"])
		return 0
	return response if response is int else 0

func subscribe(channels: Array) -> bool:
	if channels.is_empty():
		push_error("No channels provided for subscription")
		return false

	var args = ["SUBSCRIBE"]
	args.append_array(channels)

	var encoded = _encode_command(args)
	connection.put_data(encoded)

	_is_pubsub_mode = true
	_start_pubsub_polling()

	return true

func unsubscribe(channels: Array = []) -> bool:
	if not _is_pubsub_mode:
		push_warning(NOT_IN_PUB_SUB)
		return false

	var args = ["UNSUBSCRIBE"]
	if not channels.is_empty():
		args.append_array(channels)

	var encoded = _encode_command(args)
	connection.put_data(encoded)

	return true

func psubscribe(patterns: Array) -> bool:
	if patterns.is_empty():
		push_error("No patterns provided for subscription")
		return false

	var args = ["PSUBSCRIBE"]
	args.append_array(patterns)

	var encoded = _encode_command(args)
	connection.put_data(encoded)

	_is_pubsub_mode = true
	_start_pubsub_polling()

	return true

func punsubscribe(patterns: Array = []) -> bool:
	if not _is_pubsub_mode:
		push_warning(NOT_IN_PUB_SUB)
		return false

	var args = ["PUNSUBSCRIBE"]
	if not patterns.is_empty():
		args.append_array(patterns)

	var encoded = _encode_command(args)
	connection.put_data(encoded)

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
					_is_pubsub_mode = false
					_pubsub_polling = false
		"psubscribe":
			if msg.size() >= 3:
				pattern_subscribed.emit(str(msg[1]), int(msg[2]))
		"punsubscribe":
			if msg.size() >= 3:
				pattern_unsubscribed.emit(str(msg[1]), int(msg[2]))
				if int(msg[2]) == 0:
					_is_pubsub_mode = false
					_pubsub_polling = false
		"message":
			if msg.size() >= 3:
				message_received.emit(str(msg[1]), str(msg[2]))
		"pmessage":
			if msg.size() >= 4:
				pattern_message_received.emit(str(msg[1]), str(msg[2]), str(msg[3]))

func is_pubsub_mode() -> bool:
	return _is_pubsub_mode

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
		"$":
			return _parse_bulk_string(buffer)
		"*":
			return _parse_array(buffer)
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
