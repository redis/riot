package com.redislabs.riot.batch.redis.reader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPool;

import com.redislabs.riot.batch.redis.KeyValue;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LettuceKeyValueProducer implements Runnable {

	private BlockingQueue<KeyValue> queue = new LinkedBlockingDeque<>(1000);
	private int batchSize;
	private Iterator<String> keyIterator;
	private GenericObjectPool<StatefulRedisConnection<String, String>> pool;
	private long timeout;
	private boolean stopped;
	private boolean done;

	public LettuceKeyValueProducer(GenericObjectPool<StatefulRedisConnection<String, String>> pool, long timeout,
			Iterator<String> keyIterator, int batchSize) {
		this.pool = pool;
		this.timeout = timeout;
		this.keyIterator = keyIterator;
		this.batchSize = batchSize;
	}

	public void run() {
		while (keyIterator.hasNext() && !stopped) {
			List<String> keys = nextKeys();
			StatefulRedisConnection<String, String> connection;
			try {
				connection = pool.borrowObject();
			} catch (Exception e) {
				log.error("Could not get connection from pool", e);
				continue;
			}
			try {
				RedisAsyncCommands<String, String> commands = connection.async();
				commands.setAutoFlushCommands(false);
				List<RedisFuture<Long>> ttlFutures = new ArrayList<>();
				List<RedisFuture<byte[]>> valueFutures = new ArrayList<>();
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
		if (stopped) {
			return;
		}
		while (!queue.isEmpty()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				log.error("Interrupted while waiting for queue to be drained");
			}
		}
		this.done = true;
	}

	private List<String> nextKeys() {
		List<String> keys = new ArrayList<>();
		while (keyIterator.hasNext() && keys.size() < batchSize) {
			keys.add(keyIterator.next());
		}
		return keys;
	}

	public void stop() {
		this.stopped = true;
	}

	public KeyValue next() throws InterruptedException {
		if (stopped || done) {
			return null;
		}
		KeyValue value = queue.poll(100, TimeUnit.MILLISECONDS);
		if (value == null) {
			return next();
		}
		return value;
	}

}
