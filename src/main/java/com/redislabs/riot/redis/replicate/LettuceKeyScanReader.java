
package com.redislabs.riot.redis.replicate;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.redislabs.riot.redis.KeyValue;

import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@EqualsAndHashCode(callSuper = true)
@Accessors(fluent = true)
public class LettuceKeyScanReader extends AbstractItemStreamItemReader<KeyValue> {

	private @Getter @Setter long limit;
	private @Getter @Setter String match;
	private @Getter @Setter StatefulRedisConnection<String, String> connection;
	private @Getter @Setter GenericObjectPool<StatefulRedisConnection<String, String>> pool;
	private @Getter @Setter int timeout;
	private @Getter @Setter int queueCapacity;
	private @Getter @Setter int threads;
	private @Getter @Setter int batchSize;

	private Object lock = new Object();
	private KeyScanIterator iterator;
	private BlockingQueue<KeyValue> queue;
	private ExecutorService executor;

	public LettuceKeyScanReader() {
		setName(ClassUtils.getShortName(LettuceKeyScanReader.class));
	}

	@Override
	public void open(ExecutionContext executionContext) {
		synchronized (lock) {
			if (queue != null) {
				return;
			}
			ScanArgs args = new ScanArgs().limit(limit);
			if (match != null) {
				args.match(match);
			}
			iterator = new KeyScanIterator(connection, args);
			queue = new LinkedBlockingDeque<>(queueCapacity);
			executor = Executors.newFixedThreadPool(threads);
			for (int index = 0; index < threads; index++) {
				executor.submit(new KeyValueProducer(iterator, batchSize, pool, timeout, queue));
			}
			executor.shutdown();
		}
	}

	@Override
	public void close() {
		synchronized (lock) {
			executor = null;
			queue = null;
		}
	}

	@Override
	public KeyValue read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		KeyValue value;
		do {
			value = queue.poll(100, TimeUnit.MILLISECONDS);
		} while (value == null && !iterator.isFinished());
		return value;
	}

}
