package com.redis.riot;

import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;

import com.redis.riot.core.processor.RegexNamedGroupFunction;
import com.redis.riot.function.KeyValueMap;
import com.redis.spring.batch.item.redis.common.KeyValue;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public abstract class AbstractRedisExportCommand extends AbstractExportCommand {

	@ArgGroup(exclusive = false, heading = "Redis options%n")
	private RedisArgs redisArgs = new RedisArgs();

	@Option(names = "--key-regex", description = "Regex for key-field extraction, e.g. '\\w+:(?<id>.+)' extracts an id field from the key", paramLabel = "<rex>")
	private Pattern keyRegex;

	@Override
	protected RedisContext sourceRedisContext() {
		return RedisContext.of(redisArgs);
	}

	protected ItemProcessor<KeyValue<String>, Map<String, Object>> mapProcessor() {
		KeyValueMap mapFunction = new KeyValueMap();
		if (keyRegex != null) {
			mapFunction.setKey(new RegexNamedGroupFunction(keyRegex));
		}
		return new FunctionItemProcessor<>(mapFunction);
	}

	public RedisArgs getRedisArgs() {
		return redisArgs;
	}

	public void setRedisArgs(RedisArgs clientArgs) {
		this.redisArgs = clientArgs;
	}

	public Pattern getKeyRegex() {
		return keyRegex;
	}

	public void setKeyRegex(Pattern regex) {
		this.keyRegex = regex;
	}

}
