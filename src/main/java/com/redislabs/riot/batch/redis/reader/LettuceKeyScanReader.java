
package com.redislabs.riot.batch.redis.reader;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.redislabs.riot.batch.redis.KeyValue;
import com.redislabs.riot.batch.redis.LettuceConnector;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class LettuceKeyScanReader extends AbstractItemCountingItemStreamItemReader<KeyValue> {

	@Setter
	private Long count;
	@Setter
	private String match;
	private Object lock = new Object();
	private AtomicInteger activeThreads = new AtomicInteger(0);
	private LettuceKeyScanIterator iterator;
	private LettuceConnector<StatefulRedisConnection<String, String>, RedisAsyncCommands<String, String>> connector;
	private LettuceKeyValueProducer producer;
	private ExecutorService executor = Executors.newSingleThreadScheduledExecutor();

	public LettuceKeyScanReader(
			LettuceConnector<StatefulRedisConnection<String, String>, RedisAsyncCommands<String, String>> connector) {
		setName(ClassUtils.getShortName(LettuceKeyScanReader.class));
		this.connector = connector;
	}

	@Override
	protected void doOpen() throws Exception {
		synchronized (lock) {
			activeThreads.incrementAndGet();
			if (iterator != null) {
				return;
			}
			this.iterator = new LettuceKeyScanIterator(connector.getConnection().get(), count, match);
			this.producer = new LettuceKeyValueProducer(connector.getPool(), 2, iterator, 50);
			executor.submit(producer);
		}
	}

	@Override
	protected void doClose() throws Exception {
		synchronized (lock) {
			if (activeThreads.decrementAndGet() == 0) {
				this.producer.stop();
				this.producer = null;
				iterator.close();
				iterator = null;
			}
		}
	}

	@Override
	protected KeyValue doRead() throws Exception {
		return producer.next();
	}

}
