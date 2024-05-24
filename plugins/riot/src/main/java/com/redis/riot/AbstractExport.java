package com.redis.riot;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemWriter;

import com.redis.riot.core.Step;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemReader.ReaderMode;

import io.lettuce.core.RedisException;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public abstract class AbstractExport extends AbstractRedisCommand {

	public static final String NOTIFY_CONFIG = "notify-keyspace-events";
	public static final String NOTIFY_CONFIG_VALUE = "KEA";

	@ArgGroup(exclusive = false, heading = "Redis reader options%n")
	private RedisReaderArgs redisReaderArgs = new RedisReaderArgs();

	@Option(names = "--skip-config-check", description = "Skip Redis config checks like keyspace notifications.", hidden = true)
	private boolean skipConfigCheck;

	public void copyTo(AbstractExport target) {
		super.copyTo(target);
		target.redisReaderArgs = redisReaderArgs;
		target.skipConfigCheck = skipConfigCheck;
	}

	protected <K, V, T, O> Step<T, O> exportStep(RedisItemReader<K, V, T> reader, ItemWriter<O> writer) {
		Step<T, O> step = new Step<>(reader, writer);
		if (reader.getMode() != ReaderMode.LIVEONLY) {
			RedisScanSizeEstimator estimator = new RedisScanSizeEstimator(reader.getClient());
			estimator.setKeyPattern(reader.getKeyPattern());
			estimator.setKeyType(reader.getKeyType());
			log.info("Creating scan size estimator for step {} with {} and {}", step.getName(), reader.getKeyPattern(),
					reader.getKeyType());
			step.maxItemCountSupplier(estimator);
		}
		if (reader.getMode() != ReaderMode.SCAN) {
			checkNotifyConfig();
			log.info("Configuring step {} with {} and {}", step.getName(), reader.getFlushInterval(),
					reader.getIdleTimeout());
			step.flushInterval(reader.getFlushInterval());
			step.idleTimeout(reader.getIdleTimeout());
		}
		return step;
	}

	private void checkNotifyConfig() {
		if (skipConfigCheck) {
			return;
		}
		Map<String, String> valueMap;
		try {
			valueMap = redisCommands.configGet(NOTIFY_CONFIG);
		} catch (RedisException e) {
			log.warn("Could not validate keyspace notifications config", e);
			return;
		}
		String actual = valueMap.getOrDefault(NOTIFY_CONFIG, "");
		List<String> expected = NOTIFY_CONFIG_VALUE.codePoints().mapToObj(c -> String.valueOf((char) c))
				.collect(Collectors.toList());
		if (!containsAll(actual, expected)) {
			log.warn("Keyspace notifications not property configured: expected {} = '{}' but was '{}'.", NOTIFY_CONFIG,
					NOTIFY_CONFIG_VALUE, actual);
			throw new IllegalArgumentException("Keyspace notifications not properly configured");
		}
	}

	private static boolean containsAll(String value, List<String> strings) {
		for (String string : strings) {
			if (!value.contains(string)) {
				return false;
			}
		}
		return true;
	}

	@Override
	protected void configure(RedisItemReader<?, ?, ?> reader) {
		super.configure(reader);
		log.info("Configuring Redis reader with {}", redisReaderArgs);
		redisReaderArgs.configure(reader);
	}

	protected RedisScanSizeEstimator scanSizeEstimator(RedisItemReader<?, ?, ?> reader) {
		RedisScanSizeEstimator estimator = new RedisScanSizeEstimator(reader.getClient());
		estimator.setKeyPattern(reader.getKeyPattern());
		estimator.setKeyType(reader.getKeyType());
		return estimator;
	}

	public RedisReaderArgs getRedisReaderArgs() {
		return redisReaderArgs;
	}

	public void setRedisReaderArgs(RedisReaderArgs args) {
		this.redisReaderArgs = args;
	}

	public boolean isSkipConfigCheck() {
		return skipConfigCheck;
	}

	public void setSkipConfigCheck(boolean skip) {
		this.skipConfigCheck = skip;
	}

}
