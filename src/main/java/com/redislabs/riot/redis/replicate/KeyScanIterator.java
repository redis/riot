package com.redislabs.riot.redis.replicate;

import java.util.Iterator;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.StatefulRedisConnection;

public class KeyScanIterator implements Iterator<String> {

	private Object lock = new Object();
	private StatefulRedisConnection<String, String> connection;
	private ScanArgs args;
	private Iterator<String> keys;
	private KeyScanCursor<String> cursor;

	public KeyScanIterator(StatefulRedisConnection<String, String> connection, ScanArgs scanArgs) {
		this.connection = connection;
		this.args = scanArgs;
		this.cursor = connection.sync().scan(args);
		this.keys = cursor.getKeys().iterator();
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

	public boolean isFinished() {
		synchronized (lock) {
			return cursor.isFinished() && !keys.hasNext();
		}
	}

	@Override
	public String next() {
		return keys.next();
	}

}
