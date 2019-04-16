package com.redislabs.riot.redis.writer;

import java.util.Map;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.redislabs.lettusearch.RediSearchAsyncCommands;

import io.lettuce.core.RedisFuture;
import lombok.Setter;

@Setter
public class ZSetWriter extends AbstractRedisCollectionWriter implements InitializingBean {

	private String scoreField;
	private double defaultScore;

	@Override
	public void afterPropertiesSet() {
		Assert.notNull(scoreField, "No score field specified");
	}

	@Override
	protected RedisFuture<?> write(String key, String member, Map<String, Object> record,
			RediSearchAsyncCommands<String, String> commands) {
		double score = score(record);
		return commands.zadd(key, score, member);
	}

	private double score(Map<String, Object> record) {
		return converter.convert(record.getOrDefault(scoreField, defaultScore), Double.class);
	}

}
