package com.redislabs.riot.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ValueProducer<T> implements Runnable {

	private KeyIterator keyIterator;
	private int pipeline;
	private GenericObjectPool<StatefulConnection<String, String>> pool;
	private Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> asyncApi;
	private BlockingQueue<T> queue;
	private ValueReader<T> reader;

	private BlockingQueue<String> keys;
	private boolean stopped;

	@Builder
	private ValueProducer(KeyIterator keyIterator, int pipeline,
			GenericObjectPool<StatefulConnection<String, String>> pool,
			Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> asyncApi,
			BlockingQueue<T> queue, ValueReader<T> reader) {
		this.keyIterator = keyIterator;
		this.pipeline = pipeline;
		this.pool = pool;
		this.asyncApi = asyncApi;
		this.queue = queue;
		this.reader = reader;
	}

	public void stop() {
		log.debug("Producer stopped");
		this.stopped = true;
	}

	@Override
	public void run() {
		this.keys = new LinkedBlockingDeque<>(pipeline);
		try {
			while (keyIterator.hasNext() && !stopped) {
				String key = keyIterator.next();
				if (key == null) {
					continue;
				}
				keys.put(key);
				if (keys.size() >= pipeline) {
					flush();
				}
			}
			if (!keys.isEmpty()) {
				flush();
			}
		} catch (Throwable e) {
			log.error("Key/value producer encountered an error", e);
		}
	}

	public void flush() {
		List<String> keys = new ArrayList<>(this.keys.size());
		this.keys.drainTo(keys);
		try (StatefulConnection<String, String> connection = pool.borrowObject()) {
			for (T value : reader.fetch(keys, asyncApi.apply(connection))) {
				try {
					queue.put(value);
				} catch (InterruptedException e) {
					return;
				}
			}
		} catch (Exception e) {
			log.error("Could not get connection from pool for keys {}", keys, e);
		}
	}

}
