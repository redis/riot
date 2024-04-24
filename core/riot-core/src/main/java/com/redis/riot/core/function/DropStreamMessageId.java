package com.redis.riot.core.function;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.KeyValue.DataType;

import io.lettuce.core.StreamMessage;

@SuppressWarnings("unchecked")
public class DropStreamMessageId implements Consumer<KeyValue<String, Object>> {

	@Override
	public void accept(KeyValue<String, Object> t) {
		if (t.getValue() == null || KeyValue.type(t) != DataType.STREAM) {
			return;
		}
		Collection<StreamMessage<?, ?>> messages = (Collection<StreamMessage<?, ?>>) t.getValue();
		t.setValue(messages.stream().map(this::message).collect(Collectors.toList()));
	}

	@SuppressWarnings("rawtypes")
	private StreamMessage message(StreamMessage message) {
		return new StreamMessage(message.getStream(), null, message.getBody());
	}

}
