package com.redislabs.recharge.generator;

import java.util.Locale;

import org.ruaux.pojofaker.Faker;
import org.springframework.data.redis.core.StringRedisTemplate;

public class RechargeFaker extends Faker {

	private RedisValueProvider redis;

	public RechargeFaker(StringRedisTemplate template, Locale locale) {
		super(locale);
		this.redis = new RedisValueProvider(template);
	}

	public RedisValueProvider getRedis() {
		return redis;
	}

}
