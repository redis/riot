package com.redis.riot.core.function;

import java.util.function.Function;

import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.util.CodecUtils;

import io.lettuce.core.codec.RedisCodec;

public class StringKeyValueFunction<K> implements Function<KeyValue<String>, KeyValue<K>> {

    private final Function<String, K> stringKeyFunction;

    public StringKeyValueFunction(RedisCodec<K, ?> codec) {
        this.stringKeyFunction = CodecUtils.stringKeyFunction(codec);
    }

    @Override
    public KeyValue<K> apply(KeyValue<String> t) {
        KeyValue<K> result = new KeyValue<>();
        result.setKey(stringKeyFunction.apply(t.getKey()));
        result.setMemoryUsage(t.getMemoryUsage());
        result.setTtl(t.getTtl());
        result.setType(t.getType());
        result.setValue(t.getValue());
        return result;
    }

}
