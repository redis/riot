package com.redis.riot.core.processor;

import java.util.function.Function;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;

import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.codec.ByteArrayCodec;

public class KeyValueProcessor implements ItemProcessor<KeyValue<byte[]>, KeyValue<byte[]>> {

	private final Expression expression;
	private final EvaluationContext context;
	private final Function<byte[], String> toStringKeyFunction;
	private final Function<String, byte[]> stringKeyFunction;

	public KeyValueProcessor(Expression expression, EvaluationContext context) {
		this.expression = expression;
		this.context = context;
		this.toStringKeyFunction = Utils.toStringKeyFunction(ByteArrayCodec.INSTANCE);
		this.stringKeyFunction = Utils.stringKeyFunction(ByteArrayCodec.INSTANCE);
	}

	@Override
	public KeyValue<byte[]> process(KeyValue<byte[]> item) throws Exception {
		KeyValue<String> keyValue = new KeyValue<>();
		keyValue.setKey(toStringKeyFunction.apply(item.getKey()));
		keyValue.setMemoryUsage(item.getMemoryUsage());
		keyValue.setTtl(item.getTtl());
		keyValue.setType(item.getType());
		String key = expression.getValue(context, keyValue, String.class);
		item.setKey(stringKeyFunction.apply(key));
		return item;
	}

}