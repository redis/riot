package com.redis.riot;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.codec.ByteArrayCodec;

public class LoggingKeyValueWriteListener implements ItemWriteListener<KeyValue<byte[], ?>> {

	private final Logger log = LoggerFactory.getLogger(getClass());
	private final Function<byte[], String> toString = BatchUtils.toStringKeyFunction(ByteArrayCodec.INSTANCE);

	@Override
	public void afterWrite(Chunk<? extends KeyValue<byte[], ?>> chunk) {
		chunk.forEach(t -> log.info("Wrote {}", toString.apply(t.getKey())));
	}

}