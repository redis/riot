
package com.redislabs.riot.redis;

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

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RedisItemReader<T> extends AbstractItemStreamItemReader<T> {

	private KeyIterator keyIterator;
	private GenericObjectPool<StatefulConnection<String, String>> pool;
	private Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> asyncApi;
	private int queueCapacity;
	private int threads;
	private int pipeline;
	private Long flushRate;
	private ValueReader<T> reader;

	private Object lock = new Object();
	private BlockingQueue<T> queue;
	private ExecutorService executor;
	private ScheduledExecutorService scheduler;
	private List<ValueProducer<T>> producers;

	@Builder
	protected RedisItemReader(KeyIterator keyIterator, GenericObjectPool<StatefulConnection<String, String>> pool,
			Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> asyncApi,
			int queueCapacity, int threads, int pipeline, ValueReader<T> reader, Long flushRate) {
		setName(ClassUtils.getShortName(RedisItemReader.class));
		this.keyIterator = keyIterator;
		this.pool = pool;
		this.asyncApi = asyncApi;
		this.queueCapacity = queueCapacity;
		this.threads = threads;
		this.pipeline = pipeline;
		this.reader = reader;
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
			log.debug("Creating queue with capacity {}", queueCapacity);
			queue = new LinkedBlockingDeque<>(queueCapacity);
			log.debug("Creating thread pool of size {}", threads);
			executor = Executors.newFixedThreadPool(threads);
			scheduler = Executors.newSingleThreadScheduledExecutor();
			producers = new ArrayList<>(threads);
			for (int index = 0; index < threads; index++) {
				log.debug("Adding KeyValue producer");
				producers.add(ValueProducer.<T>builder().keyIterator(keyIterator).pipeline(pipeline).pool(pool)
						.asyncApi(asyncApi).queue(queue).reader(reader).build());
			}
			for (ValueProducer<T> producer : producers) {
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
	public T read() {
		T value;
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
