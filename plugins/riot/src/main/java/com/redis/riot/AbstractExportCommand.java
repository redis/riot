package com.redis.riot;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemWriter;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

import com.redis.lettucemod.RedisModulesUtils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.riot.core.AbstractJobCommand;
import com.redis.riot.core.Step;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemReader.ReaderMode;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisException;
import picocli.CommandLine.ArgGroup;

public abstract class AbstractExportCommand extends AbstractJobCommand {

	public static final String NOTIFY_CONFIG = "notify-keyspace-events";
	public static final String NOTIFY_CONFIG_VALUE = "KEA";

	private static final String TASK_NAME = "Exporting";
	private static final String STEP_NAME = "step";
	private static final String VAR_SOURCE = "source";

	@ArgGroup(exclusive = false)
	private RedisReaderArgs sourceRedisReaderArgs = new RedisReaderArgs();

	private RedisContext sourceRedisContext;

	@Override
	protected void execute() throws Exception {
		sourceRedisContext = sourceRedisContext();
		try {
			super.execute();
		} finally {
			sourceRedisContext.close();
		}
	}

	protected void configure(StandardEvaluationContext context) {
		context.setVariable(VAR_SOURCE, sourceRedisContext.getConnection().sync());
	}

	protected void configureSourceRedisReader(RedisItemReader<?, ?, ?> reader) {
		configureAsyncReader(reader);
		sourceRedisContext.configure(reader);
		log.info("Configuring source Redis reader with {}", sourceRedisReaderArgs);
		sourceRedisReaderArgs.configure(reader);
	}

	protected abstract RedisContext sourceRedisContext();

	protected <O> Step<KeyValue<String, Object>, O> step(ItemWriter<O> writer) {
		RedisItemReader<String, String, Object> reader = RedisItemReader.struct();
		configureSourceRedisReader(reader);
		Step<KeyValue<String, Object>, O> step = step(STEP_NAME, reader, writer);
		step.taskName(TASK_NAME);
		return step;
	}

	protected <K, V, T, O> Step<KeyValue<K, T>, O> step(String name, RedisItemReader<K, V, T> reader,
			ItemWriter<O> writer) {
		Step<KeyValue<K, T>, O> step = new Step<>(name, reader, writer);
		if (reader.getMode() != ReaderMode.LIVEONLY) {
			step.maxItemCountSupplier(reader.scanSizeEstimator());
		}
		if (reader.getMode() != ReaderMode.SCAN) {
			checkNotifyConfig(reader.getClient());
			step.live(true);
			step.flushInterval(reader.getFlushInterval());
			step.idleTimeout(reader.getIdleTimeout());
		}
		return step;
	}

	private void checkNotifyConfig(AbstractRedisClient client) {
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

	private Set<Character> characterSet(String string) {
		return string.codePoints().mapToObj(c -> (char) c).collect(Collectors.toSet());
	}

	public RedisReaderArgs getSourceRedisReaderArgs() {
		return sourceRedisReaderArgs;
	}

	public void setSourceRedisReaderArgs(RedisReaderArgs args) {
		this.sourceRedisReaderArgs = args;
	}

}
