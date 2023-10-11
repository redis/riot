package com.redis.riot.core.function;

import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;

import io.lettuce.core.StreamMessage;

@SuppressWarnings("unchecked")
public class DropStreamMessageIdFunction implements Function<KeyValue<String>, Object> {

    @SuppressWarnings("rawtypes")
    @Override
    public Object apply(KeyValue<String> t) {
        if (t.getType() == DataType.STREAM) {
            Collection<StreamMessage> messages = (Collection<StreamMessage>) t.getValue();
            if (!CollectionUtils.isEmpty(messages)) {
                return messages.stream().map(this::message).collect(Collectors.toList());
            }
        }
        return t.getValue();
    }

    @SuppressWarnings("rawtypes")
    private StreamMessage message(StreamMessage message) {
        return new StreamMessage(message.getStream(), null, message.getBody());
    }

}
