
package com.redislabs.riot.redis.writer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.util.ClassUtils;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractRedisWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	public static final String KEY_SEPARATOR = ":";

	protected ConversionService converter = new DefaultConversionService();
	@Setter
	private RediSearchClient redisClient;
	private ThreadLocal<StatefulRediSearchConnection<String, String>> connection = new ThreadLocal<>();
//	protected GenericObjectPool<StatefulRediSearchConnection<String, String>> pool;

	public AbstractRedisWriter() {
		setName(ClassUtils.getShortName(this.getClass()));
	}

	public String getName() {
		return getExecutionContextKey("name");
	}

//	public void setPool(GenericObjectPool<StatefulRediSearchConnection<String, String>> pool) {
//		this.pool = pool;
//	}

	protected String getValues(Map<String, Object> record, String[] fields) {
		if (fields == null || fields.length == 0) {
			return null;
		}
		StringJoiner joiner = new StringJoiner(KEY_SEPARATOR);
		for (String field : fields) {
			String value = converter.convert(record.get(field), String.class);
			joiner.add(value);
		}
		return joiner.toString();
	}

	protected Map<String, String> toStringMap(Map<String, Object> record) {
		Map<String, String> stringMap = new HashMap<String, String>();
		for (String key : record.keySet()) {
			Object value = record.get(key);
			stringMap.put(key, converter.convert(value, String.class));
		}
		return stringMap;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		connection.set(redisClient.connect());
		super.open(executionContext);
	}

	@Override
	public void write(List<? extends Map<String, Object>> records) throws Exception {
		List<RedisFuture<?>> futures = new ArrayList<>();
//		if (connection.isClosed()) {
//			return;
//		}
//		StatefulRediSearchConnection<String, String> connection = pool.borrowObject();
//		try {
		RediSearchAsyncCommands<String, String> commands = connection.get().async();
		commands.setAutoFlushCommands(false);
		for (Map<String, Object> record : records) {
			RedisFuture<?> future = write(record, commands);
			if (future != null) {
				futures.add(future);
			}
		}
		commands.flushCommands();
		try {
			boolean result = LettuceFutures.awaitAll(5, TimeUnit.SECONDS,
					futures.toArray(new RedisFuture[futures.size()]));
			if (result) {
				log.debug("Wrote {} records", records.size());
			} else {
				log.warn("Could not write {} records", records.size());
				for (RedisFuture<?> future : futures) {
					if (future.getError() != null) {
						log.error(future.getError());
					}
				}
			}
		} catch (RedisCommandExecutionException e) {
			log.error("Could not execute commands", e);
		}
//		} finally {
//			pool.returnObject(connection);
//		}
	}

	protected abstract RedisFuture<?> write(Map<String, Object> record,
			RediSearchAsyncCommands<String, String> commands);

}
