package com.redis.riot.redis;

import java.util.function.Function;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;

import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.codec.RedisCodec;

public class KeyComparisonDiffLogger<K> implements ItemWriteListener<KeyComparison<K>> {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final Function<K, String> toStringKeyFunction;

	public KeyComparisonDiffLogger(RedisCodec<K, ?> codec) {
		toStringKeyFunction = BatchUtils.toStringKeyFunction(codec);
	}

	@Override
	public void afterWrite(Chunk<? extends KeyComparison<K>> items) {
		StreamSupport.stream(items.spliterator(), false).filter(c -> c.getStatus() != Status.OK).forEach(this::log);
	}

	public void log(KeyComparison<K> comparison) {
		switch (comparison.getStatus()) {
		case MISSING:
			log.error("Missing key {}", key(comparison));
			break;
		case TYPE:
			log.error("Type mismatch on key {}. Expected {} but was {}", key(comparison),
					comparison.getSource().getType(), comparison.getTarget().getType());
			break;
		case VALUE:
			log.error("Value mismatch on key {}", key(comparison));
			break;
		case TTL:
			log.error("TTL mismatch on key {}. Expected {} but was {}", key(comparison),
					comparison.getSource().getTtl(), comparison.getTarget().getTtl());
			break;
		default:
			break;
		}
	}

	private String key(KeyComparison<K> comparison) {
		return toStringKeyFunction.apply(comparison.getSource().getKey());
	}

}
