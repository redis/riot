package com.redislabs.recharge.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.data.redis.core.StringRedisTemplate;

public class RedisValueProvider {

	private Map<String, Integer> sequenceMap = new HashMap<>();
	private Map<String, List<String>> membersMap = new HashMap<>();

	private StringRedisTemplate template;

	public RedisValueProvider(StringRedisTemplate template) {
		this.template = template;
	}

	public String randomMember(String key) {
		return template.opsForSet().randomMember(key);
	}

	public Long size(String key) {
		return template.opsForSet().size(key);
	}

	public String nextMember(String key) {
		if (!membersMap.containsKey(key)) {
			Set<String> memberSet = template.opsForSet().members(key);
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
