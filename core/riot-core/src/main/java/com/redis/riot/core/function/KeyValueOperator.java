package com.redis.riot.core.function;

import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.function.UnaryOperator;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.util.CodecUtils;

import io.lettuce.core.codec.RedisCodec;

public class KeyValueOperator<K> implements UnaryOperator<KeyValue<K>> {

    private final Function<K, String> toStringKeyFunction;

    private final Function<String, K> stringKeyFunction;

    private Function<KeyValue<String>, String> keyFunction = KeyValue::getKey;

    private Function<KeyValue<String>, String> typeFunction = KeyValue::getType;

    private ToLongFunction<KeyValue<String>> ttlFunction = KeyValue::getTtl;

    private Function<KeyValue<String>, Object> valueFunction = KeyValue::getValue;

    public KeyValueOperator(RedisCodec<K, ?> codec) {
        this.toStringKeyFunction = CodecUtils.toStringKeyFunction(codec);
        this.stringKeyFunction = CodecUtils.stringKeyFunction(codec);
    }

    public KeyValueOperator<K> key(Function<KeyValue<String>, String> keyFunction) {
        this.keyFunction = keyFunction;
        return this;
    }

    public KeyValueOperator<K> ttl(ToLongFunction<KeyValue<String>> ttlFunction) {
        this.ttlFunction = ttlFunction;
        return this;
    }

    public KeyValueOperator<K> type(Function<KeyValue<String>, String> typeFunction) {
        this.typeFunction = typeFunction;
        return this;
    }

    public KeyValueOperator<K> value(Function<KeyValue<String>, Object> valueFunction) {
        this.valueFunction = valueFunction;
        return this;
    }

    @Override
    public KeyValue<K> apply(KeyValue<K> item) {
        KeyValue<String> keyValue = new KeyValue<>();
        keyValue.setKey(toStringKeyFunction.apply(item.getKey()));
        keyValue.setMemoryUsage(item.getMemoryUsage());
        keyValue.setTtl(item.getTtl());
        keyValue.setType(item.getType());
        keyValue.setValue(item.getValue());
        String key = keyFunction.apply(keyValue);
        String type = typeFunction.apply(keyValue);
        long ttl = ttlFunction.applyAsLong(keyValue);
        Object value = valueFunction.apply(keyValue);
        item.setKey(stringKeyFunction.apply(key));
        item.setType(type);
        item.setTtl(ttl);
        item.setValue(value);
        return item;
    }

}
