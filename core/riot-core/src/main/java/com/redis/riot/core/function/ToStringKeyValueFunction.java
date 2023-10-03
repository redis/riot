package com.redis.riot.core.function;

import java.util.function.Function;

import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.util.CodecUtils;

import io.lettuce.core.codec.RedisCodec;

public class ToStringKeyValueFunction<K> implements Function<KeyValue<K>, KeyValue<String>> {

    private final Function<K, String> toStringKeyFunction;

    public ToStringKeyValueFunction(RedisCodec<K, ?> codec) {
        this.toStringKeyFunction = CodecUtils.toStringKeyFunction(codec);
    }

    @Override
    public KeyValue<String> apply(KeyValue<K> t) {
        KeyValue<String> result = new KeyValue<>();
        result.setKey(toStringKeyFunction.apply(t.getKey()));
        result.setMemoryUsage(t.getMemoryUsage());
        result.setTtl(t.getTtl());
        result.setType(t.getType());
        result.setValue(t.getValue());
        return result;
    }

}
