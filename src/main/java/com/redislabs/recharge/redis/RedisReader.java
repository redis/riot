
package com.redislabs.recharge.redis;

import java.util.List;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redislabs.lettusearch.StatefulRediSearchConnection;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;

public class RedisReader extends AbstractItemCountingItemStreamItemReader<List<String>> {

	private volatile boolean initialized = false;
	private StatefulRediSearchConnection<String, String> connection;
	private Object lock = new Object();
	private ScanConfiguration config;
	private KeyScanCursor<String> cursor;

	public RedisReader() {
		setName(ClassUtils.getShortName(RedisReader.class));
	}

	public void setConfig(ScanConfiguration config) {
		this.config = config;
	}

	public void setConnection(StatefulRediSearchConnection<String, String> connection) {
		this.connection = connection;
	}

	@Override
	protected void doOpen() throws Exception {
		Assert.state(!initialized, "Cannot open an already open ItemReader, call close first");
		ScanArgs args = new ScanArgs();
		if (config.getLimit() != null) {
			args.limit(config.getLimit());
		}
		if (config.getMatch() != null) {
			args.match(config.getMatch());
		}
		this.cursor = connection.sync().scan(args);
		initialized = true;
	}

	@Override
	protected void doClose() throws Exception {
		synchronized (lock) {
			initialized = false;
		}
	}

	@Override
	protected List<String> doRead() throws Exception {
		synchronized (lock) {
			if (cursor == null) {
				return null;
			}
			List<String> keys = cursor.getKeys();
			if (cursor.isFinished()) {
				cursor = null;
			} else {
				cursor = connection.sync().scan(cursor);
			}
			return keys;
		}
	}

}
