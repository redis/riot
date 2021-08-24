package com.redis.riot.stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public class JsonToMapConverter implements Converter<ConsumerRecord<String, Object>, Map<String, String>> {

    private final Converter<Map<String, Object>, Map<String, String>> flattener;

    public JsonToMapConverter(Converter<Map<String, Object>, Map<String, String>> flattener) {
        this.flattener = flattener;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, String> convert(ConsumerRecord<String, Object> source) {
        return flattener.convert((Map<String, Object>) source.value());
    }

}