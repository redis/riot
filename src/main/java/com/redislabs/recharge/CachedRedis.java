package com.redislabs.recharge;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.redislabs.lettusearch.RediSearchCommands;

@SuppressWarnings("rawtypes")
public class CachedRedis {

	private RediSearchCommands<String, String> commands;
	private Map<String, Map> hashes = Collections.synchronizedMap(new HashMap<>());

	public CachedRedis(RediSearchCommands<String, String> commands) {
		this.commands = commands;
	}

	public Map hgetall(String key) {
		if (hashes.containsKey(key)) {
			return hashes.get(key);
		}
		Map map = commands.hgetall(key);
		hashes.put(key, map);
		return map;
	}

}
