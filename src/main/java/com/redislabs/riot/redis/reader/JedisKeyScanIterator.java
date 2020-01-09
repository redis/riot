package com.redislabs.riot.redis.reader;

import java.util.Iterator;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

public class JedisKeyScanIterator implements Iterator<String> {

	private Jedis jedis;
	private ScanParams params = new ScanParams();
	private ScanResult<String> result;
	private int current = 0;

	public JedisKeyScanIterator(Jedis jedis, Integer count, String match) {
		this.jedis = jedis;
		this.params = new ScanParams();
		if (count != null) {
			params.count(count);
		}
		if (match != null) {
			params.match(match);
		}
		this.result = jedis.scan(ScanParams.SCAN_POINTER_START, params);
	}

	@Override
	public boolean hasNext() {
		if (current < result.getResult().size()) {
			return true;
		}
		if (result.isCompleteIteration()) {
			return false;
		}
		this.result = jedis.scan(result.getCursor(), params);
		this.current = 0;
		return hasNext();
	}

	@Override
	public String next() {
		return result.getResult().get(current++);
	}

}
