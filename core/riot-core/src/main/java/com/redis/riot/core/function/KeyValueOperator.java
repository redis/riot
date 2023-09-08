package com.redis.riot.core.function;

import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.function.UnaryOperator;

import com.redis.spring.batch.KeyValue;

public class KeyValueOperator implements UnaryOperator<KeyValue<String>> {

    private Function<KeyValue<String>, String> keyFunction = KeyValue::getKey;

    private Function<KeyValue<String>, String> typeFunction = KeyValue::getType;

    private ToLongFunction<KeyValue<String>> ttlFunction = KeyValue::getTtl;

    private Function<KeyValue<String>, Object> valueFunction = KeyValue::getValue;

    public KeyValueOperator key(Function<KeyValue<String>, String> keyFunction) {
        this.keyFunction = keyFunction;
        return this;
    }

    public KeyValueOperator ttl(ToLongFunction<KeyValue<String>> ttlFunction) {
        this.ttlFunction = ttlFunction;
        return this;
    }

    public KeyValueOperator type(Function<KeyValue<String>, String> typeFunction) {
        this.typeFunction = typeFunction;
        return this;
    }

    public KeyValueOperator value(Function<KeyValue<String>, Object> valueFunction) {
        this.valueFunction = valueFunction;
        return this;
    }

    @Override
    public KeyValue<String> apply(KeyValue<String> keyValue) {
        String key = keyFunction.apply(keyValue);
        String type = typeFunction.apply(keyValue);
        long ttl = ttlFunction.applyAsLong(keyValue);
        Object value = valueFunction.apply(keyValue);
        keyValue.setKey(key);
        keyValue.setType(type);
        keyValue.setTtl(ttl);
        keyValue.setValue(value);
        return keyValue;
    }

}
