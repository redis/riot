package com.redislabs.riot.stream.processor;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.core.convert.converter.Converter;

import lombok.Builder;

public class AvroConsumerProcessor<K> extends AbstractConsumerProcessor<K, GenericRecord> {

	@Builder
	public AvroConsumerProcessor(Converter<ConsumerRecord<K, GenericRecord>, String> keyConverter) {
		super(keyConverter);
	}

	@Override
	protected Map<String, Object> map(GenericRecord value) {
		Map<String, Object> map = new HashMap<>();
		value.getSchema().getFields().forEach(field -> map.put(field.name(), value.get(field.name())));
		return map;
	}

}
