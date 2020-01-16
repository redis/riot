package com.redislabs.riot.redis.replicate;

import java.util.Iterator;

public interface KeyIterator extends Iterator<String> {

	void start();

	void stop();

}
