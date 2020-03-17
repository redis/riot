package com.redislabs.riot.redis;

import java.util.Iterator;

public interface KeyIterator extends Iterator<String> {

	void start();

	void stop();

}
