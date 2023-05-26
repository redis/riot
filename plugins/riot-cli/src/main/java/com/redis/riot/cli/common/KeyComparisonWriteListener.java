package com.redis.riot.cli.common;

import java.util.List;

import org.springframework.batch.core.ItemWriteListener;

import com.redis.riot.core.KeyComparisonLogger;
import com.redis.spring.batch.reader.KeyComparison;

public class KeyComparisonWriteListener implements ItemWriteListener<KeyComparison> {

	private final KeyComparisonLogger logger;

	public KeyComparisonWriteListener(KeyComparisonLogger logger) {
		this.logger = logger;
	}

	@Override
	public void onWriteError(Exception exception, List<? extends KeyComparison> items) {
		// do nothing
	}

	@Override
	public void beforeWrite(List<? extends KeyComparison> items) {
		// do nothing
	}

	@Override
	public void afterWrite(List<? extends KeyComparison> items) {
		items.forEach(logger::log);
	}
}