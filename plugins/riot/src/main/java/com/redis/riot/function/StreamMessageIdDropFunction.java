package com.redis.riot.function;

import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.redis.spring.batch.item.redis.common.DataType;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.StreamMessage;

@SuppressWarnings("unchecked")
public class StreamMessageIdDropFunction implements Function<KeyValue<String, Object>, Object> {

	@SuppressWarnings("rawtypes")
	@Override
	public Object apply(KeyValue<String, Object> t) {
		Object value = t.getValue();
		if (value == null || KeyValue.type(t) != DataType.STREAM) {
			return value;
		}
		return ((Collection<StreamMessage>) value).stream().map(this::message).collect(Collectors.toList());
	}

	@SuppressWarnings("rawtypes")
	private StreamMessage message(StreamMessage message) {
		return new StreamMessage(message.getStream(), null, message.getBody());
	}

}
