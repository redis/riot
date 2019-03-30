package com.redislabs.recharge.redis.writer;

import java.util.Map;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.recharge.redis.RedisType;

import io.lettuce.core.RedisFuture;
import lombok.Setter;

@Setter
public class ZSetWriter extends AbstractCollectionRedisWriter implements InitializingBean {

	private String scoreField;
	private double defaultScore = 1d;

	@Override
	protected RedisFuture<?> write(String key, String member, Map<String, Object> record,
			RediSearchAsyncCommands<String, String> commands) {
		double score = getScore(record);
		return commands.zadd(key, score, member);
	}

	private double getScore(Map<String, Object> record) {
		return converter.convert(record.getOrDefault(scoreField, defaultScore), Double.class);
	}

	@Override
	public void afterPropertiesSet() {
		Assert.notNull(scoreField, "No score field specified");
	}

	@Override
	public RedisType getRedisType() {
		return RedisType.Zset;
	}

}
