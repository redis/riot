package com.redislabs.recharge.processor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import com.redislabs.lettusearch.RediSearchCommands;

@SuppressWarnings("rawtypes")
public class CachedRedis {

	private RediSearchCommands<String, String> commands;
	private Map<String, List<String>> sets = Collections.synchronizedMap(new HashMap<>());
	private Map<String, Map> hashes = Collections.synchronizedMap(new HashMap<>());

	public CachedRedis(RediSearchCommands<String, String> commands) {
		this.commands = commands;
	}

	public Map hgetall(String key) {
		if (!hashes.containsKey(key)) {
			hashes.put(key, Collections.unmodifiableMap(commands.hgetall(key)));
		}
		return hashes.get(key);
	}

	public String smember(String key, int index) {
		return smembers(key).get(index);
	}

	public String srandmember(String key) {
		List<String> members = smembers(key);
		return members.get(ThreadLocalRandom.current().nextInt(members.size()));
	}

	public List<String> smembers(String key) {
		if (!sets.containsKey(key)) {
			List<String> members = new ArrayList<>();
			members.addAll(commands.smembers(key));
			sets.put(key, Collections.unmodifiableList(members));
		}
		return sets.get(key);
	}

}
