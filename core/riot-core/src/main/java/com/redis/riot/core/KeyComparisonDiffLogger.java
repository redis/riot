package com.redis.riot.core;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.batch.core.ItemWriteListener;

import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;

public class KeyComparisonDiffLogger implements ItemWriteListener<KeyComparison> {

	private final Logger log;

	public KeyComparisonDiffLogger(Logger logger) {
		this.log = logger;
	}

	@Override
	public void beforeWrite(List<? extends KeyComparison> items) {
		// do nothing
	}

	@Override
	public void afterWrite(List<? extends KeyComparison> items) {
		items.stream().filter(c -> c.getStatus() != Status.OK).forEach(this::log);
	}

	@Override
	public void onWriteError(Exception exception, List<? extends KeyComparison> items) {
		// do nothing
	}

	public void log(KeyComparison comparison) {
		switch (comparison.getStatus()) {
		case MISSING:
			log("Missing key \"{0}\"", comparison.getSource().getKey());
			break;
		case TTL:
			log("TTL mismatch on key \"{0}\": {1} != {2}", comparison.getSource().getKey(),
					comparison.getSource().getTtl(), comparison.getTarget().getTtl());
			break;
		case TYPE:
			log("Type mismatch on key \"{0}\": {1} != {2}", comparison.getSource().getKey(),
					comparison.getSource().getType(), comparison.getTarget().getType());
			break;
		case VALUE:
			log("Value mismatch on {0} \"{1}\"", comparison.getSource().getType(), comparison.getSource().getKey());
			break;
		default:
			break;
		}
	}

	private void log(String msg, Object... params) {
		log.log(Level.SEVERE, msg, params);
	}
}
