package com.redis.riot.core;

import java.util.function.Function;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;

import com.redis.riot.core.function.ExpressionFunction;
import com.redis.riot.core.function.KeyValueOperator;
import com.redis.riot.core.function.LongExpressionFunction;
import com.redis.riot.core.function.StringKeyValueFunction;
import com.redis.riot.core.function.ToStringKeyValueFunction;
import com.redis.spring.batch.KeyValue;

import io.lettuce.core.codec.RedisCodec;

public class KeyValueProcessorOptions {

    private TemplateExpression keyExpression;

    private Expression typeExpression;

    private Expression ttlExpression;

    public TemplateExpression getKeyExpression() {
        return keyExpression;
    }

    public void setKeyExpression(TemplateExpression expression) {
        this.keyExpression = expression;
    }

    public Expression getTypeExpression() {
        return typeExpression;
    }

    public void setTypeExpression(Expression expression) {
        this.typeExpression = expression;
    }

    public Expression getTtlExpression() {
        return ttlExpression;
    }

    public void setTtlExpression(Expression expression) {
        this.ttlExpression = expression;
    }

    public boolean isEmpty() {
        return keyExpression == null && typeExpression == null && ttlExpression == null;
    }

    public <K> Function<KeyValue<K>, KeyValue<K>> processor(EvaluationContext context, RedisCodec<K, ?> codec) {
        KeyValueOperator operator = new KeyValueOperator();
        if (keyExpression != null) {
            operator.key(ExpressionFunction.of(context, keyExpression));
        }
        if (typeExpression != null) {
            operator.type(ExpressionFunction.of(context, typeExpression, String.class));
        }
        if (ttlExpression != null) {
            operator.ttl(new LongExpressionFunction<>(context, ttlExpression));
        }
        return new ToStringKeyValueFunction<>(codec).andThen(operator).andThen(new StringKeyValueFunction<>(codec));
    }

}
