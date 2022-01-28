package com.redis.riot.processor;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;

import com.redis.spring.batch.KeyValue;

public class KeyValueTTLProcessor<T extends KeyValue<String, ?>> implements ItemProcessor<T, T> {

    private final Expression expression;
    private final EvaluationContext context;

    public KeyValueTTLProcessor(Expression expression, EvaluationContext context) {
        this.expression = expression;
        this.context = context;
    }

    @Override
    public T process(T item) {
        item.setAbsoluteTTL(expression.getValue(context, item, Long.class));
        return item;
    }

}