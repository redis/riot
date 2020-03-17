package com.redislabs.riot.redis.replicate;

import java.util.Iterator;

import org.springframework.util.Assert;

import com.redislabs.riot.redis.KeyIterator;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.sync.RedisKeyCommands;
import lombok.Builder;

public class ScanKeyIterator implements KeyIterator {

	private RedisKeyCommands<String, String> commands;
	private ScanArgs args;
	private Object lock = new Object();
	private Iterator<String> keys;
	private KeyScanCursor<String> cursor;

	@Builder
	private ScanKeyIterator(RedisKeyCommands<String, String> commands, ScanArgs args) {
		this.commands = commands;
		this.args = args;
	}

	@Override
	public void start() {
		Assert.isNull(this.cursor, "Iterator already started");
		cursor = commands.scan(args);
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
			cursor = commands.scan(cursor, args);
			keys = cursor.getKeys().iterator();
			return keys.hasNext();
		}
	}

	@Override
	public String next() {
		return keys.next();
	}

}
