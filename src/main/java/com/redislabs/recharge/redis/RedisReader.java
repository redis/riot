
package com.redislabs.recharge.redis;

import java.util.List;
import java.util.Map;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redislabs.lettusearch.StatefulRediSearchConnection;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RedisReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	private volatile boolean initialized = false;
	private StatefulRediSearchConnection<String, String> connection;
	private Object lock = new Object();
	private RedisSourceConfiguration config;
	private KeyScanCursor<String> cursor;

	public RedisReader() {
		setName(ClassUtils.getShortName(RedisReader.class));
	}

	public void setConfig(RedisSourceConfiguration config) {
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
	protected Map<String, Object> doRead() throws Exception {
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
			for (String key : keys) {
				log.info("Key {} type: {}", key, connection.sync().type(key));
				// get type
			}
		}
		return null;
	}

}
