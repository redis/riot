package com.redis.riot.redis;

import java.util.Map;

import com.redis.spring.batch.convert.ScoredValueConverter;
import com.redis.spring.batch.writer.operation.Zadd;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "zadd", description = "Add members with scores to a sorted set")
public class ZaddCommand extends AbstractCollectionCommand {

	@Mixin
	private ZaddOptions options = new ZaddOptions();

	@Override
	public Zadd<String, String, Map<String, Object>> operation() {
		return Zadd.<String, Map<String, Object>>key(key()).value(new ScoredValueConverter<>(member(),
				numberExtractor(options.getScoreField(), Double.class, options.getScoreDefault()))).build();
	}

}
