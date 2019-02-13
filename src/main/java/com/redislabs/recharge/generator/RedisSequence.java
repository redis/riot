package com.redislabs.recharge.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.redislabs.lettusearch.RediSearchCommands;

public class RedisSequence {

	private Map<String, Integer> sequenceMap = new HashMap<>();
	private Map<String, List<String>> membersMap = new HashMap<>();
	private RediSearchCommands<String, String> commands;
	private Random random = new Random();

	public RedisSequence(RediSearchCommands<String, String> commands) {
		this.commands = commands;
	}

	public String nextMember(String key, int start, int end) {
		List<String> members = getMembers(key);
		if (members.isEmpty()) {
			return null;
		}
		Integer index = sequenceMap.getOrDefault(key, start);
		sequenceMap.put(key, (index + 1) % Math.min(members.size(), end));
		return members.get(index);
	}

	private List<String> getMembers(String key) {
		if (!membersMap.containsKey(key)) {
			Set<String> memberSet = commands.smembers(key);
			ArrayList<String> members = new ArrayList<>(memberSet.size());
			members.addAll(memberSet);
			membersMap.put(key, members);
		}
		return membersMap.get(key);
	}

	public String randomMember(String key) {
		List<String> members = getMembers(key);
		return members.get(random.nextInt(members.size()));
	}

	public String nextMember(String key) {
		return nextMember(key, 0, Integer.MAX_VALUE);
	}

}
