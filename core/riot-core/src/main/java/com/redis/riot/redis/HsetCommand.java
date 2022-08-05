package com.redis.riot.redis;

import java.util.Map;

import com.redis.spring.batch.writer.RedisOperation;
import com.redis.spring.batch.writer.operation.Hset;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "hset", aliases = "hmset", description = "Set hashes from input")
public class HsetCommand extends AbstractKeyCommand {

	@Mixin
	private FilteringOptions options = new FilteringOptions();

	public FilteringOptions getOptions() {
		return options;
	}

	public void setOptions(FilteringOptions options) {
		this.options = options;
	}

	@Override
	public RedisOperation<String, String, Map<String, Object>> operation() {
		return Hset.<String, String, Map<String, Object>>key(key()).map(options.converter()).build();
	}

}
