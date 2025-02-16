package com.redis.riot;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.reader.KeyComparison;
import com.redis.spring.batch.item.redis.reader.KeyComparisonListener;

import io.lettuce.core.codec.RedisCodec;

public class CompareLoggingWriteListener<K> implements KeyComparisonListener<K> {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final Function<K, String> toStringKeyFunction;

	public CompareLoggingWriteListener(RedisCodec<K, ?> codec) {
		toStringKeyFunction = BatchUtils.toStringKeyFunction(codec);
	}

	@Override
	public void comparison(KeyComparison<K> comparison) {
		switch (comparison.getStatus()) {
		case MISSING:
			log.error("Missing {} {}", comparison.getSource().getType(), key(comparison));
			break;
		case TYPE:
			log.error("Type mismatch on key {}. Expected {} but was {}", key(comparison),
					comparison.getSource().getType(), comparison.getTarget().getType());
			break;
		case VALUE:
			log.error("Value mismatch on {} {}", comparison.getSource().getType(), key(comparison));
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
