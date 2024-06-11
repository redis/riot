package com.redis.riot;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemWriter;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.GeoLocation;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.riot.RedisClientBuilder.RedisURIClient;
import com.redis.riot.core.AbstractJobCommand;
import com.redis.riot.core.EvaluationContextArgs;
import com.redis.riot.core.Step;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisException;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

abstract class AbstractRedisCommand extends AbstractJobCommand {

	private static final String NOTIFY_CONFIG = "notify-keyspace-events";
	private static final String NOTIFY_CONFIG_VALUE = "KEA";
	private static final String CONTEXT_VAR_REDIS = "redis";

	@ArgGroup(exclusive = false, heading = "TLS options%n")
	private SslArgs sslArgs = new SslArgs();

	@Option(names = "--client", description = "Client name used to connect to Redis (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
	private String clientName = RedisClientBuilder.DEFAULT_CLIENT_NAME;

	protected RedisURIClient client;
	protected StatefulRedisModulesConnection<String, String> connection;

	@Override
	public void afterPropertiesSet() throws Exception {
		super.afterPropertiesSet();
		if (client == null) {
			client = redisURIClient();
		}
		if (connection == null) {
			connection = RedisModulesUtils.connection(client.getClient());
		}
	}

	protected <K, V, T> RedisItemReader<K, V, T> configure(RedisItemReader<K, V, T> reader) {
		super.configure(reader);
		reader.setClient(client.getClient());
		reader.setDatabase(client.getUri().getDatabase());
		return reader;
	}

	protected abstract RedisURIClient redisURIClient();

	@Override
	protected void shutdown() {
		if (connection != null) {
			connection.close();
			connection = null;
		}
		if (client != null) {
			client.close();
			client = null;
		}
	}

	protected StandardEvaluationContext evaluationContext(EvaluationContextArgs args) {
		StandardEvaluationContext context = args.evaluationContext();
		context.setVariable(CONTEXT_VAR_REDIS, connection.sync());
		Method method;
		try {
			method = GeoLocation.class.getDeclaredMethod("toString", String.class, String.class);
		} catch (Exception e) {
			throw new UnsupportedOperationException("Could not get GeoLocation method", e);
		}
		context.registerFunction("geo", method);
		return context;
	}

	protected <K, V, T, O> Step<KeyValue<K, T>, O> step(RedisItemReader<K, V, T> reader, ItemWriter<O> writer) {
		Step<KeyValue<K, T>, O> step = new Step<>(reader, writer);
		if (shouldEstimate(reader)) {
			log.info("Creating scan size estimator for step {} with {} and {}", step.getName(), reader.getKeyPattern(),
					reader.getKeyType());
			step.maxItemCountSupplier(RedisScanSizeEstimator.from(reader));
		}
		if (isLive(reader)) {
			checkNotifyConfig(reader.getClient());
			log.info("Configuring step {} as live with {} and {}", step.getName(), reader.getFlushInterval(),
					reader.getIdleTimeout());
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

	private static Set<Character> characterSet(String string) {
		return string.codePoints().mapToObj(c -> (char) c).collect(Collectors.toSet());
	}

	private boolean shouldEstimate(RedisItemReader<?, ?, ?> reader) {
		switch (reader.getMode()) {
		case LIVE:
		case SCAN:
			return true;
		default:
			return false;
		}
	}

	private boolean isLive(RedisItemReader<?, ?, ?> reader) {
		switch (reader.getMode()) {
		case LIVE:
		case LIVEONLY:
			return true;
		default:
			return false;
		}
	}

	protected RedisClientBuilder redisClientBuilder() {
		RedisClientBuilder builder = new RedisClientBuilder();
		builder.clientName(clientName);
		builder.sslOptions(sslArgs.sslOptions());
		return builder;
	}

}
