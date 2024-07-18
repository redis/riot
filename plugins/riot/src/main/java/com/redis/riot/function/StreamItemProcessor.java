package com.redis.riot.function;

import java.util.Collection;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.item.redis.common.DataType;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.StreamMessage;

public class StreamItemProcessor implements ItemProcessor<KeyValue<String, Object>, KeyValue<String, Object>> {

	private boolean prune;
	private boolean dropMessageIds;

	@SuppressWarnings("unchecked")
	@Override
	public KeyValue<String, Object> process(KeyValue<String, Object> t) {
		if (KeyValue.hasValue(t) && KeyValue.type(t) == DataType.STREAM) {
			Collection<StreamMessage<?, ?>> messages = (Collection<StreamMessage<?, ?>>) t.getValue();
			if (CollectionUtils.isEmpty(messages)) {
				if (prune) {
					return null;
				}
			} else {
				if (dropMessageIds) {
					t.setValue(messages.stream().map(this::message).collect(Collectors.toList()));
				}
			}
		}
		return t;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private StreamMessage message(StreamMessage message) {
		return new StreamMessage(message.getStream(), null, message.getBody());
	}

	public boolean isDropMessageIds() {
		return dropMessageIds;
	}

	public void setDropMessageIds(boolean dropMessageIds) {
		this.dropMessageIds = dropMessageIds;
	}

	public boolean isPrune() {
		return prune;
	}

	public void setPrune(boolean prune) {
		this.prune = prune;
	}

}
