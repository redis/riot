package com.redis.riot;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.util.Assert;

import com.redis.lettucemod.RedisModulesUtils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.riot.core.Step;
import com.redis.riot.core.processor.RegexNamedGroupFunction;
import com.redis.riot.function.KeyValueMap;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemReader.ReaderMode;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisException;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public abstract class AbstractExportCommand<C extends RedisExecutionContext> extends AbstractRedisCommand<C> {

	private static final String NOTIFY_CONFIG = "notify-keyspace-events";
	private static final String NOTIFY_CONFIG_VALUE = "KEA";
	private static final String TASK_NAME = "Exporting";
	private static final String STEP_NAME = "step";

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

	protected <T> Step<KeyValue<String, Object>, T> step(C context, ItemWriter<T> writer) {
		RedisItemReader<String, String, Object> reader = RedisItemReader.struct();
		configure(context, reader);
		Step<KeyValue<String, Object>, T> step = new Step<>(STEP_NAME, reader, writer);
		step.taskName(TASK_NAME);
		configure(step);
		return step;
	}

	protected void configure(C context, RedisItemReader<String, String, Object> reader) {
		context.configure(reader);
		log.info("Configuring Redis reader with {}", redisReaderArgs);
		redisReaderArgs.configure(reader);
	}

	public static void configure(Step<?, ?> step) {
		Assert.isInstanceOf(RedisItemReader.class, step.getReader(),
				"Step reader must be an instance of RedisItemReader");
		RedisItemReader<?, ?, ?> reader = (RedisItemReader<?, ?, ?>) step.getReader();
		if (reader.getMode() != ReaderMode.LIVEONLY) {
			step.maxItemCountSupplier(reader.scanSizeEstimator());
		}
		if (reader.getMode() != ReaderMode.SCAN) {
			checkNotifyConfig(reader.getClient());
			step.live(true);
			step.flushInterval(reader.getFlushInterval());
			step.idleTimeout(reader.getIdleTimeout());
		}
	}

	private static void checkNotifyConfig(AbstractRedisClient client) {
		Map<String, String> valueMap;
		try (StatefulRedisModulesConnection<String, String> conn = RedisModulesUtils.connection(client)) {
			try {
				valueMap = conn.sync().configGet(NOTIFY_CONFIG);
			} catch (RedisException e) {
				return;
			}
		}
		String actual = valueMap.getOrDefault(NOTIFY_CONFIG, "");
		Set<Character> expected = characterSet(NOTIFY_CONFIG_VALUE);
		Assert.isTrue(characterSet(actual).containsAll(expected),
				String.format("Keyspace notifications not property configured: expected '%s' but was '%s'.",
						NOTIFY_CONFIG_VALUE, actual));
	}

	private static Set<Character> characterSet(String string) {
		return string.codePoints().mapToObj(c -> (char) c).collect(Collectors.toSet());
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
