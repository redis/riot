package com.redislabs.recharge.generator;

import org.springframework.data.redis.core.StringRedisTemplate;

public class RedisValueProvider {

	private StringRedisTemplate template;

	public RedisValueProvider(StringRedisTemplate template) {
		this.template = template;
	}

	public String randomMember(String key) {
		return template.opsForSet().randomMember(key);
	}

}
