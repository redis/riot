package com.redislabs.riot.redis.replicate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;

import com.redislabs.riot.redis.KeyValue;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import lombok.Builder;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
public class KeyValueProducer implements Runnable {

	private @Setter KeyIterator keyIterator;
	private @Setter int batchSize;
	private @Setter GenericObjectPool<StatefulConnection<String, String>> pool;
	private @Setter Function<StatefulConnection<String, String>, RedisAsyncCommands<String, String>> asyncApi;
	private @Setter long timeout;
	private @Setter BlockingQueue<KeyValue> queue;
	private BlockingQueue<String> keys;
	private boolean stopped;

	public void stop() {
		log.debug("Producer stopped");
		this.stopped = true;
	}

	@Override
	public void run() {
		this.keys = new LinkedBlockingDeque<>(batchSize);
		try {
			while (keyIterator.hasNext() && !stopped) {
				String key = keyIterator.next();
				if (key == null) {
					continue;
				}
				keys.put(key);
				if (keys.size() >= batchSize) {
					flush();
				}
			}
		} catch (Throwable e) {
			log.error("Key/value producer encountered an error", e);
		}
	}

	@SuppressWarnings("unchecked")
	public void flush() {
		List<String> keys = new ArrayList<>(this.keys.size());
		this.keys.drainTo(keys);
		try (StatefulConnection<String, String> connection = pool.borrowObject()) {
			BaseRedisAsyncCommands<String, String> commands = asyncApi.apply(connection);
			commands.setAutoFlushCommands(false);
			List<RedisFuture<Long>> ttlFutures = new ArrayList<>(keys.size());
			List<RedisFuture<byte[]>> valueFutures = new ArrayList<>(keys.size());
			for (String key : keys) {
				ttlFutures.add(((RedisKeyAsyncCommands<String, String>) commands).pttl(key));
				valueFutures.add(((RedisKeyAsyncCommands<String, String>) commands).dump(key));
			}
			commands.flushCommands();
			for (int index = 0; index < keys.size(); index++) {
				String key = keys.get(index);
				try {
					Long ttl = ttlFutures.get(index).get(timeout, TimeUnit.SECONDS);
					byte[] value = valueFutures.get(index).get(timeout, TimeUnit.SECONDS);
					queue.put(new KeyValue().key(key).ttl(ttl).value(value));
				} catch (Exception e) {
					log.error("Could not read value for key {}", key, e);
				}
			}
		} catch (Exception e) {
			log.error("Could not get connection from pool for keys {}", keys, e);
		}
	}

}
