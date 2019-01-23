package com.redislabs.recharge.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.redislabs.lettusearch.StatefulRediSearchConnection;

public class RedisSequence {

	private Map<String, Integer> sequenceMap = new HashMap<>();
	private Map<String, List<String>> membersMap = new HashMap<>();
	private StatefulRediSearchConnection<String, String> connection;

	public RedisSequence(StatefulRediSearchConnection<String, String> connection) {
		this.connection = connection;
	}

	public String nextMember(String key) {
		if (!membersMap.containsKey(key)) {
			Set<String> memberSet = connection.sync().smembers(key);
			ArrayList<String> members = new ArrayList<>(memberSet.size());
			members.addAll(memberSet);
			membersMap.put(key, members);
		}
		List<String> members = membersMap.get(key);
		Integer index = sequenceMap.getOrDefault(key, 0);
		sequenceMap.put(key, index + 1);
		return members.get(index);
	}

}
