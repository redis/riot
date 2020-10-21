package com.redislabs.riot.stream.processor;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.core.convert.converter.Converter;

import lombok.Builder;

public class JsonConsumerProcessor<K> extends AbstractConsumerProcessor<K, Object> {

	@Builder
	public JsonConsumerProcessor(Converter<ConsumerRecord<K, Object>, String> keyConverter) {
		super(keyConverter);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Map<String, Object> map(Object value) {
		return (Map<String, Object>) value;
	}

}
