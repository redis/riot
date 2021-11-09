package com.redis.riot.stream.kafka;

import java.util.List;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.Assert;

/**
 * @author Julien Ruaux
 *
 */
public class KafkaItemWriter<K> extends AbstractItemStreamItemWriter<ProducerRecord<K, Object>> {

	private final KafkaTemplate<K, Object> kafkaTemplate;

	public KafkaItemWriter(KafkaTemplate<K, Object> kafkaTemplate) {
		Assert.notNull(kafkaTemplate, "A Kafka template is required");
		this.kafkaTemplate = kafkaTemplate;
	}

	@Override
	public void write(List<? extends ProducerRecord<K, Object>> items) {
		for (ProducerRecord<K, Object> item : items) {
			this.kafkaTemplate.send(item);
		}
	}
}
