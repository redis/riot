package com.redis.riot.core;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;

import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.util.CodecUtils;

import io.lettuce.core.codec.RedisCodec;

public class KeyValueWriteListener<K, T extends KeyValue<K>> implements ItemWriteListener<T> {

	private final Logger log;

	private final Function<K, String> toStringKeyFunction;

	public KeyValueWriteListener(RedisCodec<K, ?> codec, Logger log) {
		this.toStringKeyFunction = CodecUtils.toStringKeyFunction(codec);
		this.log = log;
	}

	@Override
	public void beforeWrite(Chunk<? extends T> items) {
		log.debug("Writing keys {}", keys(items));
	}

	private List<String> keys(Chunk<? extends T> items) {
		return StreamSupport.stream(items.spliterator(), false).map(this::toStringKey).collect(Collectors.toList());
	}

	private String toStringKey(T item) {
		return toStringKeyFunction.apply(item.getKey());
	}

}
