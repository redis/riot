package com.redis.riot.function;

import java.util.Collection;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.StreamMessage;

public class StreamItemProcessor implements ItemProcessor<KeyValue<String>, KeyValue<String>> {

	private boolean prune;
	private boolean dropMessageIds;

	@SuppressWarnings("unchecked")
	@Override
	public KeyValue<String> process(KeyValue<String> t) {
		if (KeyValue.hasValue(t) && KeyValue.TYPE_STREAM.equals(t.getType())) {
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
