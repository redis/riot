package com.redis.riot;

import java.util.function.Function;

import org.slf4j.Logger;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.codec.RedisCodec;

public class ReplicateWriteLogger<K, T> implements ItemWriteListener<KeyValue<K, T>> {

	private final Logger logger;
	private final Function<K, String> toString;

	public ReplicateWriteLogger(Logger logger, RedisCodec<K, ?> codec) {
		this.logger = logger;
		this.toString = BatchUtils.toStringKeyFunction(codec);
	}

	protected void log(String message, Chunk<? extends KeyValue<K, ?>> items) {
		if (logger.isInfoEnabled()) {
			for (KeyValue<K, ?> item : items) {
				logger.info(message, string(item));
			}
		}
	}

	protected String string(KeyValue<K, ?> item) {
		return toString.apply(item.getKey());
	}

	@Override
	public void beforeWrite(Chunk<? extends KeyValue<K, T>> items) {
		log("Writing {}", items);
	}

	@Override
	public void afterWrite(Chunk<? extends KeyValue<K, T>> items) {
		log("Wrote {}", items);
	}

	@Override
	public void onWriteError(Exception exception, Chunk<? extends KeyValue<K, T>> items) {
		if (logger.isErrorEnabled()) {
			for (KeyValue<K, T> item : items) {
				logger.error("Could not write {}", string(item), exception);
			}
		}
	}

}