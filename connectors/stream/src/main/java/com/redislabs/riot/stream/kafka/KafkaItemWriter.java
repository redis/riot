package com.redislabs.riot.stream.kafka;

import java.util.List;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.kafka.core.KafkaTemplate;

import lombok.Builder;
import lombok.NonNull;

/**
 * @author Julien Ruaux
 *
 */
public class KafkaItemWriter<K> extends AbstractItemStreamItemWriter<ProducerRecord<K, Object>> {

	private final KafkaTemplate<K, Object> kafkaTemplate;

	@Builder
	private KafkaItemWriter(@NonNull KafkaTemplate<K, Object> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	@Override
	public void write(List<? extends ProducerRecord<K, Object>> items) {
		for (ProducerRecord<K, Object> item : items) {
			this.kafkaTemplate.send(item);
		}
	}
}
