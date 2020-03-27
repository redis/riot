package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;
import com.redislabs.riot.redis.writer.KeyBuilder;

import lombok.Builder;
import lombok.Setter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class Zadd extends AbstractCollectionMapCommandWriter {

	private @Setter String score;
	private @Setter double defaultScore;

	@Builder
	protected Zadd(KeyBuilder keyBuilder, boolean keepKeyFields, KeyBuilder memberIdBuilder, String score,
			double defaultScore) {
		super(keyBuilder, keepKeyFields, memberIdBuilder);
		this.score = score;
		this.defaultScore = defaultScore;
	}

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, String member, Map<String, Object> item) {
		return commands.zadd(redis, key, convert(item.getOrDefault(score, defaultScore), Double.class), member);
	}

}
