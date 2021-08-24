package com.redis.riot.processor;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;

public class KeyValueKeyProcessor<T extends KeyValue<?>> implements ItemProcessor<T, T> {

    private final Expression expression;
    private final EvaluationContext context;

    public KeyValueKeyProcessor(Expression expression, EvaluationContext context) {
        this.expression = expression;
        this.context = context;
    }

    @Override
    public T process(T item) {
        item.setKey(expression.getValue(context, item, String.class));
        return item;
    }

}