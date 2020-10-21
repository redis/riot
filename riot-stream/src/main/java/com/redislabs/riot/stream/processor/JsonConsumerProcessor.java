package com.redislabs.riot.stream.processor;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.core.convert.converter.Converter;

import lombok.Builder;

public class JsonConsumerProcessor<K> extends AbstractConsumerProcessor<K, Map<String, Object>> {

	@Builder
	public JsonConsumerProcessor(Converter<ConsumerRecord<K, Map<String, Object>>, String> keyConverter) {
		super(keyConverter);
	}

	@Override
	protected Map<String, Object> map(Map<String, Object> value) {
		return value;
	}

}
