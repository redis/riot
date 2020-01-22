
package com.redislabs.riot.redis.replicate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.redislabs.riot.redis.KeyValue;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KeyValueReader extends AbstractItemStreamItemReader<KeyValue> {

	private KeyIterator keyIterator;
	private GenericObjectPool<StatefulConnection<String, String>> pool;
	private Function<StatefulConnection<String, String>, RedisAsyncCommands<String, String>> asyncApi;
	private int timeout;
	private int valueQueueCapacity;
	private int threads;
	private int batchSize;
	private Long flushRate;

	private Object lock = new Object();
	private BlockingQueue<KeyValue> queue;
	private ExecutorService executor;
	private ScheduledExecutorService scheduler;
	private List<KeyValueProducer> producers;

	@Builder
	private KeyValueReader(KeyIterator keyIterator, GenericObjectPool<StatefulConnection<String, String>> pool,
			Function<StatefulConnection<String, String>, RedisAsyncCommands<String, String>> asyncApi, int timeout,
			int valueQueueCapacity, int threads, int batchSize, Long flushRate) {
		setName(ClassUtils.getShortName(KeyValueReader.class));
		this.keyIterator = keyIterator;
		this.pool = pool;
		this.asyncApi = asyncApi;
		this.timeout = timeout;
		this.valueQueueCapacity = valueQueueCapacity;
		this.threads = threads;
		this.batchSize = batchSize;
		this.flushRate = flushRate;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		synchronized (lock) {
			if (queue != null) {
				return;
			}
			log.debug("Starting key iterator");
			keyIterator.start();
			log.debug("Creating queue with capacity {}", valueQueueCapacity);
			queue = new LinkedBlockingDeque<>(valueQueueCapacity);
			log.debug("Creating thread pool of size {}", threads);
			executor = Executors.newFixedThreadPool(threads);
			scheduler = Executors.newSingleThreadScheduledExecutor();
			producers = new ArrayList<>(threads);
			for (int index = 0; index < threads; index++) {
				log.debug("Adding KeyValue producer");
				producers.add(KeyValueProducer.builder().keyIterator(keyIterator).batchSize(batchSize)
						.pool(pool).asyncApi(asyncApi).timeout(timeout).queue(queue).build());
			}
			for (KeyValueProducer producer : producers) {
				log.debug("Starting producer");
				executor.submit(producer);
				if (flushRate != null) {
					scheduler.scheduleAtFixedRate(producer::flush, flushRate, flushRate, TimeUnit.MILLISECONDS);
				}
			}
		}
	}

	@Override
	public void close() {
		synchronized (lock) {
			producers.forEach(p -> p.stop());
			scheduler.shutdown();
			scheduler = null;
			executor.shutdown();
			executor = null;
			queue = null;
			keyIterator.stop();
		}
	}

	@Override
	public KeyValue read() {
		KeyValue value;
		do {
			try {
				value = queue.poll(100, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				return null;
			}
		} while (value == null && keyIterator.hasNext());
		return value;
	}

}
