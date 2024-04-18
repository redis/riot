package com.redis.riot.core.function;

import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.function.UnaryOperator;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.KeyValue.Type;

public class KeyValueOperator implements UnaryOperator<KeyValue<String, Object>> {

	private Function<KeyValue<String, Object>, String> keyFunction = KeyValue::getKey;

	private ToLongFunction<KeyValue<String, Object>> ttlFunction = KeyValue::getTtl;

	private Function<KeyValue<String, Object>, Type> typeFunction = KeyValue::getType;

	private Function<KeyValue<String, Object>, ?> valueFunction = KeyValue::getValue;

	public void setKeyFunction(Function<KeyValue<String, Object>, String> key) {
		this.keyFunction = key;
	}

	public void setTtlFunction(ToLongFunction<KeyValue<String, Object>> ttl) {
		this.ttlFunction = ttl;
	}

	public void setTypeFunction(Function<KeyValue<String, Object>, Type> typeFunction) {
		this.typeFunction = typeFunction;
	}

	public void setValueFunction(Function<KeyValue<String, Object>, ?> value) {
		this.valueFunction = value;
	}

	@Override
	public KeyValue<String, Object> apply(KeyValue<String, Object> t) {
		t.setKey(keyFunction.apply(t));
		t.setTtl(ttlFunction.applyAsLong(t));
		t.setType(typeFunction.apply(t));
		t.setValue(valueFunction.apply(t));
		return t;
	}

}
