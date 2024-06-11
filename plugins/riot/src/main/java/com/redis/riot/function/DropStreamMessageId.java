package com.redis.riot.function;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.redis.spring.batch.item.redis.common.DataType;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.StreamMessage;

@SuppressWarnings("unchecked")
public class DropStreamMessageId implements Consumer<KeyValue<String, Object>> {

	@SuppressWarnings("rawtypes")
	@Override
	public void accept(KeyValue<String, Object> t) {
		if (KeyValue.hasValue(t) && KeyValue.type(t) == DataType.STREAM) {
			Collection<StreamMessage> messages = (Collection<StreamMessage>) t.getValue();
			t.setValue(messages.stream().map(this::message).collect(Collectors.toList()));
		}
	}

	@SuppressWarnings("rawtypes")
	private StreamMessage message(StreamMessage message) {
		return new StreamMessage(message.getStream(), null, message.getBody());
	}

}
