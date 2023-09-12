package com.redis.riot.core.function;

import java.util.function.ToLongFunction;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;

public class LongExpressionFunction<T> extends ExpressionFunction<T, Long> implements ToLongFunction<T> {

    public LongExpressionFunction(EvaluationContext context, Expression expression) {
        super(context, expression, Long.class);
    }

    @Override
    public long applyAsLong(T value) {
        Long result = getValue(value);
        if (result == null) {
            return 0;
        }
        return result;
    }

}
