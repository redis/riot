package com.redis.riot.stream.processor;

import io.lettuce.core.StreamMessage;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.converter.Converter;

public abstract class AbstractProducerProcessor
        implements ItemProcessor<StreamMessage<String, String>, ProducerRecord<String, Object>> {

    private final Converter<StreamMessage<String, String>, String> topicConverter;

    protected AbstractProducerProcessor(Converter<StreamMessage<String, String>, String> topicConverter) {
        this.topicConverter = topicConverter;
    }

    @Override
    public ProducerRecord<String, Object> process(StreamMessage<String, String> item) {
        String topic = topicConverter.convert(item);
        if (topic == null) {
            throw new IllegalStateException("Topic is null");
        }
        return new ProducerRecord<>(topic, value(item));
    }

    protected abstract Object value(StreamMessage<String, String> body);

}
