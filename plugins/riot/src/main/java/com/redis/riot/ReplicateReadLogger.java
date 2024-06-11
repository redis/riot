package com.redis.riot;

import java.util.function.Function;

import org.slf4j.Logger;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;

import com.redis.spring.batch.item.redis.common.BatchUtils;

import io.lettuce.core.codec.RedisCodec;

public class ReplicateReadLogger<K> implements ItemReadListener<K>, ItemWriteListener<K> {

	private final Logger logger;
	private final Function<K, String> toString;

	public ReplicateReadLogger(Logger logger, RedisCodec<K, ?> codec) {
		this.logger = logger;
		this.toString = BatchUtils.toStringKeyFunction(codec);
	}

	private void log(String message, Chunk<? extends K> keys) {
		if (logger.isInfoEnabled()) {
			for (K item : keys) {
				logger.info(message, string(item));
			}
		}
	}

	protected String string(K key) {
		return toString.apply(key);
	}

	@Override
	public void afterRead(K item) {
		if (logger.isInfoEnabled()) {
			logger.info("Key {}", string(item));
		}
	}

	@Override
	public void beforeWrite(Chunk<? extends K> items) {
		log("Fetching {}", items);
	}

	@Override
	public void afterWrite(Chunk<? extends K> items) {
		log("Fetched {}", items);
	}

	@Override
	public void onWriteError(Exception exception, Chunk<? extends K> items) {
		if (logger.isErrorEnabled()) {
			for (K item : items) {
				logger.error("Could not fetch {}", string(item), exception);
			}
		}
	}

}