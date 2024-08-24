package com.redis.riot;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.redis.lettucemod.RedisModulesUtils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.riot.core.Step;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemReader.ReaderMode;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisException;

public class RedisExportStep<K, V, T, O> extends Step<KeyValue<K, T>, O> implements InitializingBean {

	public static final String NOTIFY_CONFIG = "notify-keyspace-events";
	public static final String NOTIFY_CONFIG_VALUE = "KEA";

	public RedisExportStep(String name, RedisItemReader<K, V, T> reader, ItemWriter<O> writer) {
		super(name, reader, writer);
	}

	@Override
	public void afterPropertiesSet() {
		@SuppressWarnings("unchecked")
		RedisItemReader<K, V, T> reader = (RedisItemReader<K, V, T>) getReader();
		if (reader.getMode() != ReaderMode.LIVEONLY) {
			maxItemCountSupplier(reader.scanSizeEstimator());
		}
		if (reader.getMode() != ReaderMode.SCAN) {
			checkNotifyConfig(reader.getClient());
			live(true);
			flushInterval(reader.getFlushInterval());
			idleTimeout(reader.getIdleTimeout());
		}
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

}
