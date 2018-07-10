package com.redislabs.recharge.config;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

@EnableAutoConfiguration
public class RedisConfiguration {

	@Bean
	public RedisConnectionFactory connectionFactory() {
		return new JedisConnectionFactory();
	}

	@Bean
	public StringRedisTemplate redisTemplate(RedisConnectionFactory connectionFactory) {
		StringRedisTemplate template = new StringRedisTemplate();
		template.setConnectionFactory(connectionFactory);
		return template;
	}

}