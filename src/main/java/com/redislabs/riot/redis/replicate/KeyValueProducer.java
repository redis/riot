package com.redislabs.riot.redis.replicate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPool;

import com.redislabs.riot.redis.KeyValue;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Accessors(fluent = true)
@RequiredArgsConstructor
public class KeyValueProducer implements Runnable {

	private Object lock = new Object();
	private final KeyScanIterator iterator;
	private final int batchSize;
	private final GenericObjectPool<StatefulRedisConnection<String, String>> pool;
	private final long timeout;
	private final BlockingQueue<KeyValue> queue;

	@Override
	public void run() {
		List<String> keys;
		while (!(keys = nextKeys()).isEmpty()) {
			try {
				addValues(keys);
			} catch (Exception e) {
				log.error("Could not get values for keys {}", keys);
			}
		}
	}

	private void addValues(List<String> keys) throws Exception {
		StatefulRedisConnection<String, String> connection = pool.borrowObject();
		try {
			RedisAsyncCommands<String, String> commands = connection.async();
			commands.setAutoFlushCommands(false);
			List<RedisFuture<Long>> ttlFutures = new ArrayList<>(keys.size());
			List<RedisFuture<byte[]>> valueFutures = new ArrayList<>(keys.size());
			for (String key : keys) {
				ttlFutures.add(commands.pttl(key));
				valueFutures.add(commands.dump(key));
			}
			commands.flushCommands();
			for (int index = 0; index < valueFutures.size(); index++) {
				try {
					Long ttl = ttlFutures.get(index).get(timeout, TimeUnit.SECONDS);
					byte[] value = valueFutures.get(index).get(timeout, TimeUnit.SECONDS);
					queue.put(new KeyValue(keys.get(index), ttl, value));
				} catch (Exception e) {
					log.error("Could not read key {}", keys.get(index), e);
				}
			}
		} finally {
			pool.returnObject(connection);
		}
	}

	private List<String> nextKeys() {
		synchronized (lock) {
			List<String> keys = new ArrayList<>(batchSize);
			while (iterator.hasNext() && keys.size() < batchSize) {
				keys.add(iterator.next());
			}
			return keys;
		}
	}

}
