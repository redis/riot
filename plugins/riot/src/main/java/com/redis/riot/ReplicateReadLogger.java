package com.redis.riot;

import java.util.function.Function;

import org.slf4j.Logger;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.reader.KeyEvent;

import io.lettuce.core.codec.RedisCodec;

public class ReplicateReadLogger<K> implements ItemReadListener<KeyEvent<K>>, ItemWriteListener<KeyEvent<K>> {

	private final Logger logger;
	private final Function<K, String> toString;

	public ReplicateReadLogger(Logger logger, RedisCodec<K, ?> codec) {
		this.logger = logger;
		this.toString = BatchUtils.toStringKeyFunction(codec);
	}

	private void log(String format, KeyEvent<K> keyEvent) {
		logger.info(format, string(keyEvent));
	}

	protected String string(KeyEvent<K> key) {
		return toString.apply(key.getKey());
	}

	private void log(String format, Iterable<? extends KeyEvent<K>> keys) {
		if (logger.isInfoEnabled()) {
			keys.forEach(k -> log(format, k));
		}
	}

	@Override
	public void afterRead(KeyEvent<K> item) {
		if (logger.isInfoEnabled()) {
			log("Key {}", item);
		}
	}

	@Override
	public void beforeWrite(Chunk<? extends KeyEvent<K>> items) {
		log("Fetching {}", items);
	}

	@Override
	public void afterWrite(Chunk<? extends KeyEvent<K>> items) {
		log("Fetched {}", items);
	}

	@Override
	public void onWriteError(Exception exception, Chunk<? extends KeyEvent<K>> items) {
		if (logger.isErrorEnabled()) {
			for (KeyEvent<K> item : items) {
				logger.error("Could not fetch {}", string(item), exception);
			}
		}
	}

}