package com.redis.riot.core.function;

import java.util.function.Function;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.util.CodecUtils;

import io.lettuce.core.codec.RedisCodec;

public class StringKeyValueFunction<K> implements Function<KeyValue<String>, KeyValue<K>> {

    private final Function<String, K> stringKeyFunction;

    public StringKeyValueFunction(RedisCodec<K, ?> codec) {
        this.stringKeyFunction = CodecUtils.stringKeyFunction(codec);
    }

    @Override
    public KeyValue<K> apply(KeyValue<String> item) {
        KeyValue<K> keyValue = new KeyValue<>();
        keyValue.setKey(stringKeyFunction.apply(item.getKey()));
        keyValue.setMemoryUsage(item.getMemoryUsage());
        keyValue.setTtl(item.getTtl());
        keyValue.setType(item.getType());
        keyValue.setValue(item.getValue());
        return keyValue;
    }

}
