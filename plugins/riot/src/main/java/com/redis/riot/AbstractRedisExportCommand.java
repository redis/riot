package com.redis.riot;

import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;

import com.redis.riot.core.processor.RegexNamedGroupFunction;
import com.redis.riot.function.KeyValueMap;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public abstract class AbstractRedisExportCommand extends AbstractExportCommand {

	@ArgGroup(exclusive = false)
	private SimpleRedisArgs redisArgs = new SimpleRedisArgs();

	@Option(names = "--pool", description = "Max number of Redis connections in pool (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int poolSize = RedisItemWriter.DEFAULT_POOL_SIZE;

	@Option(names = "--key-regex", description = "Regex for key-field extraction, e.g. '\\w+:(?<id>.+)' extracts an id field from the key", paramLabel = "<rex>")
	private Pattern keyRegex;

	@Override
	protected RedisContext sourceRedisContext() {
		return redisArgs.redisContext();
	}

	@Override
	protected void configureSourceRedisReader(RedisItemReader<?, ?, ?> reader) {
		super.configureSourceRedisReader(reader);
		log.info("Configuring Redis reader with poolSize {}", poolSize);
		reader.setPoolSize(poolSize);
	}

	protected ItemProcessor<KeyValue<String, Object>, Map<String, Object>> mapProcessor() {
		KeyValueMap mapFunction = new KeyValueMap();
		if (keyRegex != null) {
			mapFunction.setKey(new RegexNamedGroupFunction(keyRegex));
		}
		return new FunctionItemProcessor<>(mapFunction);
	}

	public SimpleRedisArgs getRedisArgs() {
		return redisArgs;
	}

	public void setRedisArgs(SimpleRedisArgs clientArgs) {
		this.redisArgs = clientArgs;
	}

	public int getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	public Pattern getKeyRegex() {
		return keyRegex;
	}

	public void setKeyRegex(Pattern regex) {
		this.keyRegex = regex;
	}

}
