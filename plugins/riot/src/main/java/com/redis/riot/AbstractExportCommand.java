package com.redis.riot;

import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.function.FunctionItemProcessor;

import com.redis.riot.core.Step;
import com.redis.riot.core.processor.RegexNamedGroupFunction;
import com.redis.riot.function.KeyValueMap;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.common.KeyValue;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public abstract class AbstractExportCommand extends AbstractRedisArgsCommand {

	private static final String TASK_NAME = "Exporting";
	private static final String STEP_NAME = "export";

	@ArgGroup(exclusive = false)
	private RedisReaderArgs redisReaderArgs = new RedisReaderArgs();

	@Option(names = "--key-regex", description = "Regex for key-field extraction, e.g. '\\w+:(?<id>.+)' extracts an id field from the key", paramLabel = "<rex>")
	private Pattern keyRegex;

	protected ItemProcessor<KeyValue<String, Object>, Map<String, Object>> mapProcessor() {
		KeyValueMap mapFunction = new KeyValueMap();
		if (keyRegex != null) {
			mapFunction.setKey(new RegexNamedGroupFunction(keyRegex));
		}
		return new FunctionItemProcessor<>(mapFunction);
	}

	protected <T> Step<KeyValue<String, Object>, T> step(ItemWriter<T> writer) {
		Step<KeyValue<String, Object>, T> step = new Step<>(STEP_NAME, reader(), writer).taskName(TASK_NAME);
		configureExportStep(step);
		return step;
	}

	private RedisItemReader<String, String, Object> reader() {
		RedisItemReader<String, String, Object> reader = RedisItemReader.struct();
		configure(reader);
		log.info("Configuring Redis reader with {}", redisReaderArgs);
		redisReaderArgs.configure(reader);
		return reader;
	}

	public RedisReaderArgs getRedisReaderArgs() {
		return redisReaderArgs;
	}

	public void setRedisReaderArgs(RedisReaderArgs args) {
		this.redisReaderArgs = args;
	}

	public Pattern getKeyRegex() {
		return keyRegex;
	}

	public void setKeyRegex(Pattern regex) {
		this.keyRegex = regex;
	}
}
