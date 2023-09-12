package com.redis.riot.core.function;

import java.util.function.Function;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.util.CodecUtils;

import io.lettuce.core.codec.RedisCodec;

public class ToStringKeyValueFunction<K> implements Function<KeyValue<K>, KeyValue<String>> {

    private final Function<K, String> toStringKeyFunction;

    public ToStringKeyValueFunction(RedisCodec<K, ?> codec) {
        this.toStringKeyFunction = CodecUtils.toStringKeyFunction(codec);
    }

    @Override
    public KeyValue<String> apply(KeyValue<K> item) {
        KeyValue<String> keyValue = new KeyValue<>();
        keyValue.setKey(toStringKeyFunction.apply(item.getKey()));
        keyValue.setMemoryUsage(item.getMemoryUsage());
        keyValue.setTtl(item.getTtl());
        keyValue.setType(item.getType());
        keyValue.setValue(item.getValue());
        return keyValue;
    }

}
