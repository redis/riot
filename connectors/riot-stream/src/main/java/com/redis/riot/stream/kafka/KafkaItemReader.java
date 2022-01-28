/*
 * Copyright 2019 the original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.redis.riot.stream.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.util.Assert;

import com.redis.spring.batch.reader.PollableItemReader;

/**
 * <p>
 * An {@link org.springframework.batch.item.ItemReader} implementation for
 * Apache Kafka. Uses a {@link KafkaConsumer} to read data from a given topic.
 * Multiple partitions within the same topic can be assigned to this reader.
 * </p>
 * <p>
 * Since {@link KafkaConsumer} is not thread-safe, this reader is not
 * thead-safe.
 * </p>
 *
 * @author Mathieu Ouellet
 * @author Mahmoud Ben Hassine
 * @since 4.2
 */
public class KafkaItemReader<K, V> extends AbstractItemStreamItemReader<ConsumerRecord<K, V>>
		implements PollableItemReader<ConsumerRecord<K, V>> {

	private static final String TOPIC_PARTITION_OFFSETS = "topic.partition.offsets";

	private static final String PROPERTY_MUST_BE_PROVIDED = " property must be provided";

	private final List<TopicPartition> topicPartitions;
	private final Properties consumerProperties;
	private Map<TopicPartition, Long> partitionOffsets;
	private KafkaConsumer<K, V> kafkaConsumer;
	private Iterator<ConsumerRecord<K, V>> consumerRecords;
	private boolean saveState = true;

	/**
	 * Create a new {@link KafkaItemReader}.
	 * <p>
	 * <strong>{@code consumerProperties} must contain the following keys:
	 * 'bootstrap.servers', 'group.id', 'key.deserializer' and 'value.deserializer'
	 * </strong>
	 * </p>
	 * .
	 *
	 * @param consumerProperties properties of the consumer
	 * @param topicName          name of the topic to read data from
	 * @param partitions         list of partitions to read data from
	 */
	public KafkaItemReader(Properties consumerProperties, String topicName, List<Integer> partitions) {
		Assert.notNull(consumerProperties, "Consumer properties must not be null");
		Assert.isTrue(consumerProperties.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG + PROPERTY_MUST_BE_PROVIDED);
		Assert.isTrue(consumerProperties.containsKey(ConsumerConfig.GROUP_ID_CONFIG),
				ConsumerConfig.GROUP_ID_CONFIG + PROPERTY_MUST_BE_PROVIDED);
		Assert.isTrue(consumerProperties.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG),
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG + PROPERTY_MUST_BE_PROVIDED);
		Assert.isTrue(consumerProperties.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG),
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG + PROPERTY_MUST_BE_PROVIDED);
		this.consumerProperties = consumerProperties;
		Assert.hasLength(topicName, "Topic name must not be null or empty");
		Assert.notEmpty(partitions, "At least one partition must be provided");
		this.topicPartitions = new ArrayList<>();
		for (Integer partition : partitions) {
			this.topicPartitions.add(new TopicPartition(topicName, partition));
		}
	}

	/**
	 * Set the flag that determines whether to save internal data for
	 * {@link ExecutionContext}. Only switch this to false if you don't want to save
	 * any state from this stream, and you don't need it to be restartable. Always
	 * set it to false if the reader is being used in a concurrent environment.
	 *
	 * @param saveState flag value (default true).
	 */
	public void setSaveState(boolean saveState) {
		this.saveState = saveState;
	}

	/**
	 * The flag that determines whether to save internal state for restarts.
	 *
	 * @return true if the flag was set
	 */
	public boolean isSaveState() {
		return this.saveState;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		this.kafkaConsumer = new KafkaConsumer<>(this.consumerProperties);
		this.partitionOffsets = new HashMap<>();
		for (TopicPartition topicPartition : this.topicPartitions) {
			this.partitionOffsets.put(topicPartition, 0L);
		}
		if (this.saveState && executionContext.containsKey(TOPIC_PARTITION_OFFSETS)) {
			Map<TopicPartition, Long> offsets = (Map<TopicPartition, Long>) executionContext
					.get(TOPIC_PARTITION_OFFSETS);
			if (offsets != null) {
				for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
					this.partitionOffsets.put(entry.getKey(), entry.getValue() == 0 ? 0 : entry.getValue() + 1);
				}
			}
		}
		this.kafkaConsumer.assign(this.topicPartitions);
		this.partitionOffsets.forEach(this.kafkaConsumer::seek);
		super.open(executionContext);
	}

	@Override
	public ConsumerRecord<K, V> poll(long timeout, TimeUnit unit) {
		if (this.consumerRecords == null || !this.consumerRecords.hasNext()) {
			this.consumerRecords = this.kafkaConsumer
					.poll(Duration.ofMillis(unit.convert(timeout, TimeUnit.MILLISECONDS))).iterator();
		}
		if (this.consumerRecords.hasNext()) {
			ConsumerRecord<K, V> consumerRecord = this.consumerRecords.next();
			this.partitionOffsets.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
					consumerRecord.offset());
			return consumerRecord;
		} else {
			return null;
		}
	}

	@Override
	public ConsumerRecord<K, V> read() throws Exception {
		throw new IllegalAccessException("read method is not supposed to be called");
	}

	@Override
	public void update(ExecutionContext executionContext) {
		if (this.saveState) {
			executionContext.put(TOPIC_PARTITION_OFFSETS, new HashMap<>(this.partitionOffsets));
		}
		this.kafkaConsumer.commitSync();
	}

	@Override
	public void close() {
		super.close();
		if (this.kafkaConsumer != null) {
			this.kafkaConsumer.close();
		}
	}

}
