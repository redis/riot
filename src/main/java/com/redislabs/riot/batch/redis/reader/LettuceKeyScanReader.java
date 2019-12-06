
package com.redislabs.riot.batch.redis.reader;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.redislabs.riot.batch.redis.KeyValue;

import io.lettuce.core.api.StatefulRedisConnection;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Accessors(fluent = true)
public class LettuceKeyScanReader extends AbstractItemCountingItemStreamItemReader<KeyValue> {

	@Setter
	private StatefulRedisConnection<String, String> connection;
	@Setter
	private GenericObjectPool<StatefulRedisConnection<String, String>> pool;
	@Setter
	private Long count;
	@Setter
	private String match;
	private LettuceKeyScanIterator iterator;
	private LettuceKeyValueProducer producer;
	private ExecutorService executor = Executors.newSingleThreadScheduledExecutor();

	public LettuceKeyScanReader() {
		setName(ClassUtils.getShortName(LettuceKeyScanReader.class));
	}

	@Override
	protected void doOpen() throws Exception {
		if (iterator != null) {
			return;
		}
		this.iterator = new LettuceKeyScanIterator(connection, count, match);
		this.producer = new LettuceKeyValueProducer(pool, 2, iterator, 50);
		this.executor.submit(producer);
		this.executor.shutdown();
	}

	@Override
	protected void doClose() throws Exception {
		this.producer.stop();
		int timeout = 5;
		log.debug("Waiting up to {} seconds for executor termination", timeout);
		boolean terminated = this.executor.awaitTermination(timeout, TimeUnit.SECONDS);
		if (!terminated) {
			log.warn("Forcing executor shutdown");
			this.executor.shutdownNow();
		}
		this.producer = null;
		iterator.close();
		iterator = null;
	}

	@Override
	protected KeyValue doRead() throws Exception {
		return producer.next();
	}

}
