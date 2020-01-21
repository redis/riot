package com.redislabs.riot.redis.replicate;

import java.util.Iterator;

import org.springframework.util.Assert;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.Setter;

public class ScanKeyIterator implements KeyIterator {

	private @Setter StatefulRedisConnection<String, String> connection;
	private @Setter ScanArgs args;

	private Object lock = new Object();
	private Iterator<String> keys;
	private KeyScanCursor<String> cursor;

	@Override
	public void start() {
		Assert.isNull(this.cursor, "Iterator already started");
		cursor = connection.sync().scan(args);
		keys = cursor.getKeys().iterator();
	}

	@Override
	public void stop() {
		cursor = null;
		keys = null;
	}

	@Override
	public boolean hasNext() {
		synchronized (lock) {
			if (keys.hasNext()) {
				return true;
			}
			if (cursor.isFinished()) {
				return false;
			}
			cursor = connection.sync().scan(cursor, args);
			keys = cursor.getKeys().iterator();
			return keys.hasNext();
		}
	}

	@Override
	public String next() {
		return keys.next();
	}

}
