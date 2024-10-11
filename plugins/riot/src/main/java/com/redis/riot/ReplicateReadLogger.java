package com.redis.riot;

import java.util.function.Function;

import org.slf4j.Logger;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.codec.RedisCodec;

public class ReplicateReadLogger<K> implements ItemReadListener<KeyValue<K>>, ItemWriteListener<KeyValue<K>> {

	private final Logger logger;
	private final Function<K, String> toString;

	public ReplicateReadLogger(Logger logger, RedisCodec<K, ?> codec) {
		this.logger = logger;
		this.toString = BatchUtils.toStringKeyFunction(codec);
	}

	private void log(String format, Iterable<? extends KeyValue<K>> keys) {
		if (logger.isInfoEnabled()) {
			keys.forEach(k -> log(format, k));
		}
	}

	private void log(String format, KeyValue<K> keyEvent) {
		logger.info(format, string(keyEvent));
	}

	protected String string(KeyValue<K> key) {
		return toString.apply(key.getKey());
	}

	@Override
	public void afterRead(KeyValue<K> item) {
		if (logger.isInfoEnabled()) {
			log("Key {}", item);
		}
	}

	@Override
	public void beforeWrite(Chunk<? extends KeyValue<K>> items) {
		log("Fetching {}", items);
	}

	@Override
	public void afterWrite(Chunk<? extends KeyValue<K>> items) {
		log("Fetched {}", items);
	}

	@Override
	public void onWriteError(Exception exception, Chunk<? extends KeyValue<K>> items) {
		if (logger.isErrorEnabled()) {
			for (KeyValue<K> item : items) {
				logger.error("Could not fetch {}", string(item), exception);
			}
		}
	}

}