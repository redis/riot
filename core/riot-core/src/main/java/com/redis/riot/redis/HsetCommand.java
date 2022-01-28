package com.redis.riot.redis;

import java.util.Map;

import com.redis.spring.batch.writer.RedisOperation;
import com.redis.spring.batch.writer.operation.Hset;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "hset", aliases = "hmset", description = "Set hashes from input")
public class HsetCommand extends AbstractKeyCommand {

	@CommandLine.Mixin
	private FilteringOptions filtering = new FilteringOptions();

	@Override
	public RedisOperation<String, String, Map<String, Object>> operation() {
		return Hset.<String, String, Map<String, Object>>key(key()).map(filtering.converter()).build();
	}

}
