package com.redis.riot.core.function;

import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.function.UnaryOperator;

import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;

public class KeyValueOperator implements UnaryOperator<KeyValue<String>> {

    private Function<KeyValue<String>, String> keyFunction = KeyValue::getKey;

    private ToLongFunction<KeyValue<String>> ttlFunction = KeyValue::getTtl;

    private Function<KeyValue<String>, DataType> typeFunction = KeyValue::getType;

    private Function<KeyValue<String>, Object> valueFunction = KeyValue::getValue;

    public void setKeyFunction(Function<KeyValue<String>, String> key) {
        this.keyFunction = key;
    }

    public void setTtlFunction(ToLongFunction<KeyValue<String>> ttl) {
        this.ttlFunction = ttl;
    }

    public void setTypeFunction(Function<KeyValue<String>, DataType> typeFunction) {
        this.typeFunction = typeFunction;
    }

    public void setValueFunction(Function<KeyValue<String>, Object> value) {
        this.valueFunction = value;
    }

    @Override
    public KeyValue<String> apply(KeyValue<String> t) {
        t.setKey(keyFunction.apply(t));
        t.setTtl(ttlFunction.applyAsLong(t));
        t.setType(typeFunction.apply(t));
        t.setValue(valueFunction.apply(t));
        return t;
    }

}
