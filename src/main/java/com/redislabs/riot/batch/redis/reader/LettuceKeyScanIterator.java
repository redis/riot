package com.redislabs.riot.batch.redis.reader;

import java.util.Iterator;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

public class LettuceKeyScanIterator implements Iterator<String> {

	private StatefulRedisConnection<String, String> connection;
	private RedisCommands<String, String> commands;
	private KeyScanCursor<String> cursor;
	private Iterator<String> keys;
	private ScanArgs args = new ScanArgs();
	private boolean closed;

	public LettuceKeyScanIterator(StatefulRedisConnection<String, String> connection, Long count, String match) {
		this.connection = connection;
		this.commands = connection.sync();
		if (count != null) {
			args.limit(count);
		}
		if (match != null) {
			args.match(match);
		}
		this.cursor = commands.scan(args);
		this.keys = cursor.getKeys().iterator();
	}

	public void close() {
		this.closed = true;
		this.connection.close();
	}

	@Override
	public synchronized boolean hasNext() {
		if (closed) {
			return false;
		}
		if (keys.hasNext()) {
			return true;
		}
		if (cursor.isFinished()) {
			return false;
		}
		this.cursor = commands.scan(cursor, args);
		this.keys = cursor.getKeys().iterator();
		return hasNext();
	}

	@Override
	public synchronized String next() {
		return keys.next();
	}

}
