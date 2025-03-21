package com.redis.riot;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.Assert;

import com.redis.lettucemod.RedisModulesUtils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.riot.core.Step;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemReader.ReaderMode;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisException;

public class ExportStepHelper {

	public static final String NOTIFY_CONFIG = "notify-keyspace-events";
	public static final String NOTIFY_CONFIG_VALUE = "KEA";

	private final Logger log;

	public ExportStepHelper(Logger log) {
		this.log = log;
	}

	public <K, V, T, O> Step<KeyValue<K>, O> step(RedisItemReader<K, V> reader, ItemWriter<O> writer) {
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

}
