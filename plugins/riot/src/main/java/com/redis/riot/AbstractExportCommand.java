package com.redis.riot;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.springframework.batch.item.ItemWriter;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

import com.redis.lettucemod.RedisModulesUtils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.riot.core.AbstractJobCommand;
import com.redis.riot.core.RiotInitializationException;
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
	private static final String VAR_SOURCE = "source";

	@ArgGroup(exclusive = false)
	private RedisReaderArgs sourceRedisReaderArgs = new RedisReaderArgs();

	private RedisContext sourceRedisContext;

	@Override
	protected void initialize() throws RiotInitializationException {
		super.initialize();
		sourceRedisContext = sourceRedisContext();
		sourceRedisContext.afterPropertiesSet();
	}

	@Override
	protected void teardown() {
		if (sourceRedisContext != null) {
			sourceRedisContext.close();
		}
		super.teardown();
	}

	protected void configure(StandardEvaluationContext context) {
		context.setVariable(VAR_SOURCE, sourceRedisContext.getConnection().sync());
	}

	protected void configureSourceRedisReader(RedisItemReader<?, ?> reader) {
		configureAsyncReader(reader);
		sourceRedisContext.configure(reader);
		log.info("Configuring {} with {}", reader.getName(), sourceRedisReaderArgs);
		sourceRedisReaderArgs.configure(reader);
	}

	protected abstract RedisContext sourceRedisContext();

	protected <O> Step<KeyValue<String>, O> step(ItemWriter<O> writer) {
		RedisItemReader<String, String> reader = RedisItemReader.struct();
		configureSourceRedisReader(reader);
		Step<KeyValue<String>, O> step = step(reader, writer);
		step.taskName(TASK_NAME);
		return step;
	}

	protected <K, V, T, O> Step<KeyValue<K>, O> step(RedisItemReader<K, V> reader, ItemWriter<O> writer) {
		Step<KeyValue<K>, O> step = new Step<>(reader, writer);
		if (reader.getMode() == ReaderMode.SCAN) {
			log.info("Configuring step with scan size estimator");
			step.maxItemCountSupplier(reader.scanSizeEstimator());
		} else {
			checkNotifyConfig(reader.getClient(), log);
			log.info("Configuring export step with live true, flushInterval {}, idleTimeout {}",
					reader.getFlushInterval(), reader.getIdleTimeout());
			step.live(true);
			step.flushInterval(reader.getFlushInterval());
			step.idleTimeout(reader.getIdleTimeout());
		}
		return step;
	}

	public static void checkNotifyConfig(AbstractRedisClient client, Logger log) {
		Map<String, String> valueMap;
		try (StatefulRedisModulesConnection<String, String> conn = RedisModulesUtils.connection(client)) {
			try {
				valueMap = conn.sync().configGet(NOTIFY_CONFIG);
			} catch (RedisException e) {
				log.info("Could not check keyspace notification config", e);
				return;
			}
		}
		String actual = valueMap.getOrDefault(NOTIFY_CONFIG, "");
		log.info("Retrieved config {}: {}", NOTIFY_CONFIG, actual);
		Set<Character> expected = characterSet(NOTIFY_CONFIG_VALUE);
		Assert.isTrue(characterSet(actual).containsAll(expected),
				String.format("Keyspace notifications not property configured. Expected %s '%s' but was '%s'.",
						NOTIFY_CONFIG, NOTIFY_CONFIG_VALUE, actual));
	}

	private static Set<Character> characterSet(String string) {
		return string.codePoints().mapToObj(c -> (char) c).collect(Collectors.toSet());
	}

	public RedisReaderArgs getSourceRedisReaderArgs() {
		return sourceRedisReaderArgs;
	}

	public void setSourceRedisReaderArgs(RedisReaderArgs args) {
		this.sourceRedisReaderArgs = args;
	}

}
