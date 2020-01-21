package com.redislabs.riot.redis.replicate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPool;

import com.redislabs.riot.redis.KeyValue;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KeyValueProducer implements Runnable {

	private KeyIterator keyIterator;
	private int batchSize;
	private GenericObjectPool<StatefulRedisConnection<String, String>> pool;
	private long timeout;
	private BlockingQueue<KeyValue> queue;
	private boolean stopped;
	private BlockingQueue<String> keys;

	public KeyValueProducer(KeyIterator keyIterator, int batchSize,
			GenericObjectPool<StatefulRedisConnection<String, String>> pool, long timeout,
			BlockingQueue<KeyValue> queue) {
		this.keyIterator = keyIterator;
		this.batchSize = batchSize;
		this.pool = pool;
		this.timeout = timeout;
		this.queue = queue;
	}

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

	public void flush() {
		List<String> keys = new ArrayList<>(this.keys.size());
		this.keys.drainTo(keys);
		try (StatefulRedisConnection<String, String> connection = pool.borrowObject()) {
			RedisAsyncCommands<String, String> commands = connection.async();
			commands.setAutoFlushCommands(false);
			List<RedisFuture<Long>> ttlFutures = new ArrayList<>(keys.size());
			List<RedisFuture<byte[]>> valueFutures = new ArrayList<>(keys.size());
			for (String key : keys) {
				ttlFutures.add(commands.pttl(key));
				valueFutures.add(commands.dump(key));
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
