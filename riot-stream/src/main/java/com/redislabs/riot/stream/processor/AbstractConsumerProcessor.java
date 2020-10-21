package com.redislabs.riot.stream.processor;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.vault.support.JsonMapFlattener;

import io.lettuce.core.StreamMessage;

public abstract class AbstractConsumerProcessor<K, V>
		implements ItemProcessor<ConsumerRecord<K, V>, StreamMessage<String, String>> {

	private final Converter<ConsumerRecord<K, V>, String> keyConverter;

	protected AbstractConsumerProcessor(Converter<ConsumerRecord<K, V>, String> keyConverter) {
		this.keyConverter = keyConverter;
	}

	@Override
	public StreamMessage<String, String> process(ConsumerRecord<K, V> record) throws Exception {
		return new StreamMessage<>(keyConverter.convert(record), null,
				JsonMapFlattener.flattenToStringMap(map(record.value())));
	}

	protected abstract Map<String, Object> map(V value);

}
