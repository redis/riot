
package com.redislabs.riot.redis;

import java.util.List;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redislabs.lettusearch.StatefulRediSearchConnection;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RedisReader extends AbstractItemCountingItemStreamItemReader<RedisEntry> {

	private StatefulRediSearchConnection<String, String> connection;
	private ScanArgs args = new ScanArgs();
	private volatile boolean initialized = false;
	private KeyScanCursor<String> cursor;
	private Object lock = new Object();

	public RedisReader() {
		setName(ClassUtils.getShortName(RedisReader.class));
	}

	public void setLimit(Long limit) {
		this.args.limit(limit);
	}

	public void setMatch(String match) {
		this.args.match(match);
	}

	public void setConnection(StatefulRediSearchConnection<String, String> connection) {
		this.connection = connection;
	}

	@Override
	protected void doOpen() throws Exception {
		Assert.state(!initialized, "Cannot open an already open ItemReader, call close first");
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
	protected RedisEntry doRead() throws Exception {
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
