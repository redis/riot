package com.redis.riot.core.file;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.function.UnaryOperator;

import com.redis.spring.batch.KeyValue;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class FileDumpFunction implements UnaryOperator<KeyValue<String>> {

    @Override
    public KeyValue<String> apply(KeyValue<String> item) {
        if (item.getType() == null) {
            return null;
        }
        item.setValue(value(item));
        return item;
    }

    @SuppressWarnings("unchecked")
    private Object value(KeyValue<String> item) {
        switch (item.getType()) {
            case KeyValue.ZSET:
                Collection<Map<String, Object>> zset = (Collection<Map<String, Object>>) item.getValue();
                Collection<ScoredValue<String>> values = new ArrayList<>(zset.size());
                for (Map<String, Object> map : zset) {
                    double score = ((Number) map.get("score")).doubleValue();
                    String value = (String) map.get("value");
                    values.add((ScoredValue<String>) ScoredValue.fromNullable(score, value));
                }
                return values;
            case KeyValue.STREAM:
                Collection<Map<String, Object>> stream = (Collection<Map<String, Object>>) item.getValue();
                Collection<StreamMessage<String, String>> messages = new ArrayList<>(stream.size());
                for (Map<String, Object> message : stream) {
                    messages.add(new StreamMessage<>((String) message.get("stream"), (String) message.get("id"),
                            (Map<String, String>) message.get("body")));
                }
                return messages;
            default:
                return item.getValue();
        }
    }

}
