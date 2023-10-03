package com.redis.riot.core.function;

import java.util.Collection;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;

import io.lettuce.core.StreamMessage;

@SuppressWarnings("unchecked")
public class StreamMessageIdDropOperator implements UnaryOperator<KeyValue<String>> {

    @SuppressWarnings("rawtypes")
    @Override
    public KeyValue<String> apply(KeyValue<String> t) {
        if (t.getType() == DataType.STREAM) {
            Collection<StreamMessage> messages = (Collection<StreamMessage>) t.getValue();
            t.setValue(messages.stream().map(m -> new StreamMessage(m.getStream(), null, m.getBody()))
                    .collect(Collectors.toList()));
        }
        return t;
    }

}
